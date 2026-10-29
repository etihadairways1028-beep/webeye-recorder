// IVAO Pulse track recorder.
//
// Every minute a cron trigger asks one SQLite-backed Durable Object to fetch IVAO's public
// Whazzup feed and save every connected pilot's position. The map then asks
// GET /tracks/<sessionId> for a flight's full track from pushback. No API key is involved:
// it only reads the same public feed the map uses.
//
// It also carries ATC clearances: a controller proves which station they are by putting a
// one-time code in their ATIS (read back from the public feed), then sends squawks and taxi
// routes that the pilot's Pulse picks up. See "ATC clearances" below.
//
// It also keeps an automatic logbook (takeoff → landing per pilot), touchdowns sent by the
// Pulse Link sim add-on, and weekly leaderboards (hours, flights, landings, ATC time).
//
// Free-plan budget (limits: 100k rows written, 5M rows read per day):
//  • positions go into a fixed ring of 16 rows × 1,080 minutes, overwritten in place (≈23k writes/day,
//    no deletes); new sessions ≈5k; two rows per landing; ATC every 5 minutes. ≈35k writes/day.
//  • the ring is kept in memory once read, so track and replay lookups normally cost no reads.
import { DurableObject } from 'cloudflare:workers';

const WHAZZUP = 'https://api.ivao.aero/v2/tracker/whazzup';
const SHARDS = 16; // pilots are spread over 16 rows per minute
const KEEP_MIN = 18 * 60; // keep 18 hours: enough for the longest flights
const SLOTS = KEEP_MIN + 10; // ring slots per shard (minute % SLOTS)
const ATC_KEEP_MIN = 8 * 1440; // ATC snapshots kept 8 days (weekly ATC leaderboard)
const REC = 17; // bytes per pilot: u32 session, f32 lat, f32 lon, u16 alt/2, u16 gs, u8 flags

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Max-Age': '86400',
};

function json(body, status = 200, extra = {}) {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json', ...CORS, ...extra } });
}

export class Recorder extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    // Positions: a ring per shard, slot = minute % SLOTS, overwritten in place (no deletes).
    this.sql.exec(`CREATE TABLE IF NOT EXISTS ring (shard INTEGER NOT NULL, slot INTEGER NOT NULL, minute INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (shard, slot)) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID`);
    // ATC: pending verifications, verified sessions, and the latest clearance per pilot.
    this.sql.exec(`CREATE TABLE IF NOT EXISTS atc_pending (callsign TEXT PRIMARY KEY, vid INTEGER, code TEXT, expires INTEGER) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS atc_session (token TEXT PRIMARY KEY, callsign TEXT, vid INTEGER, atc_id INTEGER, position TEXT, created INTEGER) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS clearance (pilot TEXT PRIMARY KEY, session INTEGER, data TEXT, at INTEGER, ack INTEGER, from_cs TEXT) WITHOUT ROWID`);
    // Logbook: completed flights by VID.
    this.sql.exec(`CREATE TABLE IF NOT EXISTS logbook (vid INTEGER NOT NULL, landed INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (vid, landed)) WITHOUT ROWID`);
    // Touchdowns sent by the Pulse Link sim add-on, and weekly totals for the leaderboards.
    this.sql.exec(`CREATE TABLE IF NOT EXISTS touchdown (session INTEGER NOT NULL, at INTEGER NOT NULL, vid INTEGER, data TEXT, PRIMARY KEY (session, at)) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS weekly (week INTEGER NOT NULL, vid INTEGER NOT NULL, minutes INTEGER, flights INTEGER, best_fpm INTEGER, callsign TEXT, PRIMARY KEY (week, vid)) WITHOUT ROWID`);
    // Replay: who each session is (written once per new session) and ATC online every 5 minutes.
    this.sql.exec(`CREATE TABLE IF NOT EXISTS sess (session INTEGER PRIMARY KEY, vid INTEGER, callsign TEXT, type TEXT, dep TEXT, arr TEXT, seen INTEGER) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS atc_snap (minute INTEGER PRIMARY KEY, data TEXT) WITHOUT ROWID`);
    this.known = null; // sessions already in sess
    this.mem = new Map(); // shard -> Map<minute, Uint8Array>, filled on first use
    this.last = null; // { minute, pilots } of the latest recording
    this.takeoffs = new Map(); // session -> takeoff minute (in memory; recovered from the track if lost)
    this.atcAgg = null; // cached ATC minutes per VID
    this.prev = null; // session -> onGround at the previous minute
    this.tdLog = new Map();
    this.lbCache = null;
    this.feed = null;
    this.feedAt = 0;
    this.sendLog = new Map();
  }

  /** Called by the cron trigger once a minute. */
  async record() {
    // TEST_FAKE_MINUTES (local testing only): each run counts as the next minute.
    const minute = this.env && this.env.TEST_FAKE_MINUTES ? (this.latestMinute() || Math.floor(Date.now() / 60000) - 60) + 1 : Math.floor(Date.now() / 60000);
    let pilots;
    try {
      const res = await fetch((this.env && this.env.WHAZZUP_URL) || WHAZZUP, { headers: { Accept: 'application/json', 'User-Agent': 'IVAO-Pulse-recorder/1.0 (+community)' } });
      if (!res.ok) throw new Error(`Whazzup HTTP ${res.status}`);
      const j = await res.json();
      pilots = (j && j.clients && j.clients.pilots) || [];
      this.feed = j;
      this.feedAt = Date.now();
    } catch (e) {
      this.setMeta({ lastError: String(e && e.message ? e.message : e), lastErrorAt: new Date().toISOString() });
      return { ok: false };
    }

    // Pack each pilot into its shard's buffer.
    const buckets = Array.from({ length: SHARDS }, () => []);
    for (const p of pilots) {
      const t = p && p.lastTrack;
      if (!t || typeof t.latitude !== 'number' || typeof t.longitude !== 'number' || typeof p.id !== 'number') continue;
      buckets[p.id % SHARDS].push(p);
    }
    let n = 0;
    for (let s = 0; s < SHARDS; s++) {
      const list = buckets[s];
      const buf = new ArrayBuffer(list.length * REC);
      const dv = new DataView(buf);
      list.forEach((p, i) => {
        const t = p.lastTrack;
        const o = i * REC;
        dv.setUint32(o, p.id >>> 0);
        dv.setFloat32(o + 4, t.latitude);
        dv.setFloat32(o + 8, t.longitude);
        dv.setUint16(o + 12, Math.max(0, Math.min(65535, Math.round((Number(t.altitude) || 0) / 2))));
        dv.setUint16(o + 14, Math.max(0, Math.min(65535, Math.round(Number(t.groundSpeed) || 0))));
        dv.setUint8(o + 16, t.onGround ? 1 : 0);
      });
      this.writeShard(s, minute, buf);
      n += list.length;
    }

    // Replay: remember who new sessions are (one row per new session, not per minute).
    try {
      if (!this.known) this.known = new Set(this.sql.exec('SELECT session FROM sess WHERE seen > ?', (minute - KEEP_MIN - 120) * 60000).toArray().map((r) => r.session));
      for (const p of pilots) {
        if (typeof p.id !== 'number' || this.known.has(p.id)) continue;
        const fp = p.flightPlan || {};
        this.sql.exec('INSERT OR IGNORE INTO sess (session, vid, callsign, type, dep, arr, seen) VALUES (?, ?, ?, ?, ?, ?, ?)', p.id, p.userId || 0, String(p.callsign || '').slice(0, 12), (fp.aircraft && fp.aircraft.icaoCode) || fp.aircraftId || null, fp.departureId || null, fp.arrivalId || null, minute * 60000);
        this.known.add(p.id);
      }
      if (minute % 5 === 0) {
        const atc = this.atcList(this.feed).filter((a) => a.position !== 'ATIS' && a.position !== 'OBS').map((a) => {
          const raw = ((this.feed.clients && this.feed.clients.atcs) || []).find((x) => x.id === a.id) || {};
          const t = raw.lastTrack || {};
          return [a.callsign, a.position, a.frequency || null, typeof t.latitude === 'number' ? Math.round(t.latitude * 1e3) / 1e3 : null, typeof t.longitude === 'number' ? Math.round(t.longitude * 1e3) / 1e3 : null, a.vid || 0];
        });
        this.sql.exec('INSERT OR REPLACE INTO atc_snap (minute, data) VALUES (?, ?)', minute, JSON.stringify(atc));
      }
    } catch (e) {
      this.setMeta({ replayError: String(e && e.message ? e.message : e) });
    }

    // Logbook: compare each pilot's on-ground flag with the previous minute.
    try {
      this.detectFlights(pilots, minute);
    } catch (e) {
      this.setMeta({ logbookError: String(e && e.message ? e.message : e) });
    }
    // Network history for the stats charts: pilots and ATC online, every 10 minutes for 18 h.
    if (minute % 10 === 0) {
      const hist = JSON.parse(this.getMeta('hist') || '[]').filter((h) => h[0] > minute - KEEP_MIN);
      hist.push([minute, n, ((this.feed && this.feed.clients && this.feed.clients.atcs) || []).length]);
      this.setMeta({ hist: JSON.stringify(hist) });
    }

    // Prune once every 10 minutes, shard by shard along the key (cheap on rows read).
    if (minute % 10 === 0) {
      const cutoff = minute - KEEP_MIN;
      const now = Date.now();
      this.sql.exec('DELETE FROM atc_pending WHERE expires < ?', now);
      this.sql.exec('DELETE FROM atc_session WHERE created < ?', now - 24 * 3600e3);
      this.sql.exec('DELETE FROM clearance WHERE at < ?', now - 6 * 3600e3);
      this.sql.exec('DELETE FROM atc_snap WHERE minute < ?', minute - ATC_KEEP_MIN);
      if (minute % 120 === 0) {
        this.sql.exec('DELETE FROM sess WHERE seen < ?', (minute - KEEP_MIN - 24 * 60) * 60000);
        this.known = null;
      }
      if (minute % 1440 === 0) {
        this.sql.exec('DELETE FROM touchdown WHERE at < ?', now - 48 * 3600e3);
        this.sql.exec('DELETE FROM weekly WHERE week < ?', weekOf(now) - 12);
        this.cleanupOld(minute);
      }
    }
    // Status lives in memory; written to storage only every 10 minutes (saves ~2.8k writes a day).
    this.last = { minute, pilots: n };
    if (minute % 10 === 0) this.setMeta({ lastMinute: String(minute), pilots: String(n) });
    return { ok: true, pilots: n };
  }

  getMeta(k) {
    const r = this.sql.exec('SELECT v FROM meta WHERE k = ?', k).toArray()[0];
    return r ? r.v : null;
  }

  /** Takeoff = on ground last minute, airborne now; landing = the reverse. Rows are written only on those changes. */
  detectFlights(pilots, minute) {
    const now = minute * 60000;
    const cur = new Map();
    for (const p of pilots) {
      const t = p && p.lastTrack;
      if (!t || typeof p.id !== 'number') continue;
      cur.set(p.id, !!t.onGround);
    }
    if (!this.prev) {
      // First run after a restart: rebuild the previous minute from the last snapshot.
      this.prev = new Map();
      for (let sh = 0; sh < SHARDS; sh++) {
        const u8 = this.frame(sh, minute - 1);
        if (!u8) continue;
        const dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
        for (let i = 0; i + REC <= dv.byteLength; i += REC) this.prev.set(dv.getUint32(i), dv.getUint8(i + 16) === 1);
      }
    }
    for (const p of pilots) {
      if (!cur.has(p.id) || !this.prev.has(p.id)) continue;
      const was = this.prev.get(p.id), is = cur.get(p.id);
      const fp = p.flightPlan || {};
      if (was && !is) {
        this.takeoffs.set(p.id, minute); // memory only: no write at takeoff
      } else if (!was && is) {
        let to = this.takeoffs.get(p.id);
        this.takeoffs.delete(p.id);
        if (to == null) to = this.findTakeoff(p.id, minute); // lost on a restart: look it up in the track
        if (to == null) continue;
        const takeoff = to * 60000;
        if (now - takeoff < 3 * 60000) continue; // a bounce, not a flight
        const info = { callsign: p.callsign, dep: fp.departureId || null, arr: fp.arrivalId || null, aircraft: (fp.aircraft && fp.aircraft.icaoCode) || fp.aircraftId || null, online: p.createdAt || null };
        const flight = this.summarise(p, info, takeoff, now);
        if (!flight) continue;
        this.sql.exec('INSERT OR REPLACE INTO logbook (vid, landed, data) VALUES (?, ?, ?)', p.userId, now, JSON.stringify(flight));
        const best = flight.touchdown ? Math.abs(flight.touchdown.fpm) : null;
        this.sql.exec(
          'INSERT INTO weekly (week, vid, minutes, flights, best_fpm, callsign) VALUES (?, ?, ?, 1, ?, ?) ON CONFLICT (week, vid) DO UPDATE SET minutes = minutes + excluded.minutes, flights = flights + 1, callsign = excluded.callsign, best_fpm = CASE WHEN excluded.best_fpm IS NULL THEN best_fpm WHEN best_fpm IS NULL THEN excluded.best_fpm ELSE MIN(best_fpm, excluded.best_fpm) END',
          weekOf(now), p.userId, flight.airborneMin, best, flight.callsign,
        );
        this.lbCache = null;
      }
    }
    for (const id of this.takeoffs.keys()) if (!cur.has(id)) this.takeoffs.delete(id);
    this.prev = cur;
  }

  /** Last ground → air change in this session's track before `minute` (minute number), or null. */
  findTakeoff(sessionId, minute) {
    const pts = this.track(sessionId, 0);
    for (let i = pts.length - 1; i > 0; i--) {
      const t = new Date(pts[i].timestamp).getTime() / 60000;
      if (t >= minute) continue;
      if (!pts[i].onGround && pts[i - 1].onGround) return t;
    }
    return null;
  }

  summarise(p, info, takeoff, landed) {
    const pts = this.track(p.id, takeoff - 120e3).filter((q) => new Date(q.timestamp).getTime() <= landed + 60e3);
    let dist = 0, maxAlt = 0;
    for (let i = 1; i < pts.length; i++) {
      const a = pts[i - 1], b = pts[i];
      const R = Math.PI / 180;
      const d = 2 * Math.asin(Math.sqrt(Math.sin(((b.latitude - a.latitude) * R) / 2) ** 2 + Math.cos(a.latitude * R) * Math.cos(b.latitude * R) * Math.sin(((b.longitude - a.longitude) * R) / 2) ** 2));
      dist += d * 3440.065;
    }
    for (const q of pts) maxAlt = Math.max(maxAlt, q.altitude || 0);
    // Final descent: the last two airborne reports before touchdown (one a minute) — an estimate.
    const air = pts.filter((q) => !q.onGround);
    let descent = null;
    if (air.length >= 2) {
      const x = air[air.length - 2], y = air[air.length - 1];
      const mins = (new Date(y.timestamp) - new Date(x.timestamp)) / 60e3;
      if (mins > 0 && mins <= 3) descent = Math.round((y.altitude - x.altitude) / mins);
    }
    const step = Math.max(1, Math.ceil(pts.length / 90));
    const route = pts.filter((_, i) => i % step === 0 || i === pts.length - 1).map((q) => [Math.round(q.longitude * 1e4) / 1e4, Math.round(q.latitude * 1e4) / 1e4, Math.round((q.altitude || 0) / 100)]);
    const t = p.lastTrack || {};
    // The add-on's touchdown for this flight (the last one, so touch-and-goes don't count).
    const td = this.sql.exec('SELECT data FROM touchdown WHERE session = ? AND at >= ? AND at <= ? ORDER BY at DESC LIMIT 1', p.id, takeoff, landed + 3 * 60e3).toArray()[0];
    const touchdown = td ? JSON.parse(td.data) : null;
    return { ...info, takeoff, landed, touchdown, airborneMin: Math.round((landed - takeoff) / 60e3), distanceNm: Math.round(dist), maxAlt, descentFpm: descent, landedLat: t.latitude, landedLon: t.longitude, route };
  }

  logbookFor(vid) {
    const rows = this.sql.exec('SELECT landed, data FROM logbook WHERE vid = ? ORDER BY landed DESC LIMIT 300', vid).toArray();
    // Flying right now: straight from the live feed (nothing stored at takeoff).
    const active = ((this.feed && this.feed.clients && this.feed.clients.pilots) || [])
      .filter((p) => p.userId === vid && p.lastTrack && !p.lastTrack.onGround)
      .map((p) => {
        const fp = p.flightPlan || {};
        const to = this.takeoffs.get(p.id);
        return { session: p.id, callsign: p.callsign, dep: fp.departureId || null, arr: fp.arrivalId || null, aircraft: (fp.aircraft && fp.aircraft.icaoCode) || fp.aircraftId || null, takeoff: to != null ? to * 60000 : null };
      });
    return { flights: rows.map((r) => JSON.parse(r.data)), active, atcMinutes: this.atcMinutes().total.get(vid) || 0 };
  }

  /** Touchdown from the Pulse Link add-on. Accepted only if that VID is flying on IVAO right there. */
  async touchdown(body) {
    const vid = Number(body.vid);
    if (!Number.isInteger(vid) || vid <= 0) return { status: 400, body: { error: 'Missing VID.' } };
    const now = Date.now();
    if (now - (this.tdLog.get(vid) || 0) < 20e3) return { status: 429, body: { error: 'Too many touchdowns.' } };
    const num = (v, lo, hi) => (typeof v === 'number' && Number.isFinite(v) && v >= lo && v <= hi ? v : null);
    const lat = num(body.lat, -90, 90), lon = num(body.lon, -180, 180), fpm = num(body.fpm, -5000, 500);
    if (lat == null || lon == null || fpm == null) return { status: 400, body: { error: 'Bad touchdown.' } };
    const feed = await this.getFeed();
    const p = ((feed.clients && feed.clients.pilots) || []).find((x) => x.userId === vid);
    if (!p || !p.lastTrack) return { status: 404, body: { error: `VID ${vid} isn't flying on IVAO right now.` } };
    const R = Math.PI / 180, t = p.lastTrack;
    const d = 3440 * 2 * Math.asin(Math.sqrt(Math.sin(((t.latitude - lat) * R) / 2) ** 2 + Math.cos(lat * R) * Math.cos(t.latitude * R) * Math.sin(((t.longitude - lon) * R) / 2) ** 2));
    if (d > 20) return { status: 409, body: { error: 'Your sim position does not match your IVAO position.' } };
    const data = {
      fpm: Math.round(fpm),
      g: num(body.g, 0, 6) != null ? Math.round(body.g * 100) / 100 : null,
      ias: num(body.ias, 0, 400) != null ? Math.round(body.ias) : null,
      pitch: num(body.pitch, -30, 30) != null ? Math.round(body.pitch * 10) / 10 : null,
      bank: num(body.bank, -60, 60) != null ? Math.round(body.bank * 10) / 10 : null,
      fuelUsedKg: num(body.fuelUsedKg, 0, 300000) != null ? Math.round(body.fuelUsedKg) : null,
      bounces: num(body.bounces, 0, 20) != null ? Math.round(body.bounces) : 0,
      sim: clean(body.sim, 24),
      // Local testing runs on fake minutes; keep the touchdown on the same clock.
      at: this.env && this.env.TEST_FAKE_MINUTES ? ((this.last && this.last.minute) || 0) * 60000 + 30e3 : now,
    };
    this.sql.exec('INSERT OR REPLACE INTO touchdown (session, at, vid, data) VALUES (?, ?, ?, ?)', p.id, data.at, vid, JSON.stringify(data));
    this.tdLog.set(vid, now);
    return { status: 200, body: { ok: true, callsign: p.callsign, ...data } };
  }

  /**
   * Replay frames: every pilot's position for `n` minutes from `from`, as base64 of the packed
   * 17-byte records, plus who each session is and the ATC online at the start.
   */
  replay(from, n) {
    const out = { minutes: [], frames: [], info: {}, atc: [] };
    const ids = new Set();
    for (let m = from; m < from + n; m++) {
      const bufs = [];
      for (let sh = 0; sh < SHARDS; sh++) {
        const u8 = this.frame(sh, m);
        if (u8) bufs.push(u8);
      }
      if (!bufs.length) continue;
      const len = bufs.reduce((a, b) => a + b.length, 0);
      const all = new Uint8Array(len);
      let o = 0;
      for (const b of bufs) all.set(b, o), (o += b.length);
      const dv = new DataView(all.buffer);
      for (let i = 0; i + REC <= len; i += REC) ids.add(dv.getUint32(i));
      out.minutes.push(m);
      out.frames.push(b64(all));
    }
    if (ids.size) {
      const list = [...ids];
      for (let i = 0; i < list.length; i += 400) {
        const chunk = list.slice(i, i + 400);
        for (const r of this.sql.exec(`SELECT session, vid, callsign, type, dep, arr FROM sess WHERE session IN (${chunk.map(() => '?').join(',')})`, ...chunk).toArray()) out.info[r.session] = [r.callsign, r.vid, r.type, r.dep, r.arr];
      }
    }
    const a = this.sql.exec('SELECT minute, data FROM atc_snap WHERE minute <= ? ORDER BY minute DESC LIMIT 1', from + Math.floor(n / 2)).toArray()[0];
    if (a) out.atc = JSON.parse(a.data);
    const mins = [...this.shardMap(0).keys()];
    out.oldest = mins.length ? Math.min(...mins) : null;
    if (!this.noOldSnap) {
      try {
        const o = this.sql.exec('SELECT MIN(minute) AS m FROM snap WHERE shard = 0 AND minute >= ?', Math.floor(Date.now() / 60000) - KEEP_MIN).toArray()[0];
        if (o && o.m && (out.oldest == null || o.m < out.oldest)) out.oldest = o.m;
      } catch {
        this.noOldSnap = true;
      }
    }
    out.latest = this.latestMinute();
    return out;
  }

  /** This week's (or last week's) top pilots and controllers. */
  leaderboard(which) {
    const week = weekOf(Date.now()) - (which === 'last' ? 1 : 0);
    const key = `${week}`;
    if (this.lbCache && this.lbCache.key === key && Date.now() - this.lbCache.at < 300e3) return this.lbCache.body;
    const rows = (q) => this.sql.exec(q, week).toArray();
    const body = {
      week,
      from: new Date(week * 7 * 864e5 - 3 * 864e5).toISOString(),
      hours: rows('SELECT vid, callsign, minutes, flights FROM weekly WHERE week = ? ORDER BY minutes DESC LIMIT 10'),
      flights: rows('SELECT vid, callsign, minutes, flights FROM weekly WHERE week = ? ORDER BY flights DESC, minutes DESC LIMIT 10'),
      landings: rows('SELECT vid, callsign, best_fpm AS fpm FROM weekly WHERE week = ? AND best_fpm IS NOT NULL ORDER BY best_fpm ASC LIMIT 10'),
      atc: [...(this.atcMinutes().weeks.get(week) || new Map()).entries()].map(([vid, v]) => ({ vid, callsign: v.callsign, minutes: v.minutes })).sort((a, b) => b.minutes - a.minutes).slice(0, 10),
    };
    this.lbCache = { key, at: Date.now(), body };
    return body;
  }

  stats() {
    return { hist: JSON.parse(this.getMeta('hist') || '[]').map(([m, p, a]) => ({ t: new Date(m * 60000).toISOString(), pilots: p, atc: a })) };
  }

  setMeta(obj) {
    for (const [k, v] of Object.entries(obj)) this.sql.exec('INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)', k, v);
  }

  status() {
    const out = { storage: 'ring' };
    for (const r of this.sql.exec('SELECT k, v FROM meta').toArray()) out[r.k] = r.v;
    const latest = this.latestMinute();
    if (latest) out.lastMinute = String(latest);
    if (this.last) out.pilots = String(this.last.pilots);
    if (latest) out.lastRecorded = new Date(latest * 60000).toISOString();
    return out;
  }

  latestMinute() {
    if (this.last) return this.last.minute;
    const r = this.sql.exec('SELECT MAX(minute) AS m FROM ring WHERE shard = 0').toArray()[0];
    return (r && r.m) || Number(this.getMeta('lastMinute')) || null;
  }

  // ───────── position storage (ring + memory)

  writeShard(shard, minute, buf) {
    this.sql.exec('INSERT INTO ring (shard, slot, minute, data) VALUES (?, ?, ?, ?) ON CONFLICT (shard, slot) DO UPDATE SET minute = excluded.minute, data = excluded.data', shard, minute % SLOTS, minute, buf);
    const m = this.mem.get(shard);
    if (m) {
      m.set(minute, new Uint8Array(buf));
      m.delete(minute - SLOTS);
      for (const k of m.keys()) if (k <= minute - SLOTS) m.delete(k);
    }
  }

  /** All recent minutes of one shard, read from storage once and then kept in memory. */
  shardMap(shard) {
    let m = this.mem.get(shard);
    if (m) return m;
    m = new Map();
    for (const r of this.sql.exec('SELECT minute, data FROM ring WHERE shard = ?', shard).toArray()) m.set(r.minute, toU8(r.data));
    this.mem.set(shard, m);
    return m;
  }

  /** One shard at one minute (memory if loaded, else a single key lookup). */
  frame(shard, minute) {
    const m = this.mem.get(shard);
    let u8 = null;
    if (m) u8 = m.get(minute) || null;
    else {
      const r = this.sql.exec('SELECT minute, data FROM ring WHERE shard = ? AND slot = ?', shard, minute % SLOTS).toArray()[0];
      if (r && r.minute === minute) u8 = toU8(r.data);
    }
    if (u8 || this.noOldSnap) return u8;
    // Switching from the old storage: minutes from before the ring existed.
    try {
      const o = this.sql.exec('SELECT data FROM snap WHERE shard = ? AND minute = ?', shard, minute).toArray()[0];
      return o ? toU8(o.data) : null;
    } catch {
      this.noOldSnap = true;
      return null;
    }
  }

  /** ATC minutes from the 5-minute snapshots: total per VID (8 days) and per week. Cached 10 min. */
  atcMinutes() {
    if (this.atcAgg && Date.now() - this.atcAgg.at < 600e3) return this.atcAgg;
    const total = new Map();
    const weeks = new Map();
    const since = Math.floor(Date.now() / 60000) - ATC_KEEP_MIN;
    for (const r of this.sql.exec('SELECT minute, data FROM atc_snap WHERE minute >= ?', since).toArray()) {
      const week = weekOf(r.minute * 60000);
      let w = weeks.get(week);
      if (!w) weeks.set(week, (w = new Map()));
      for (const a of JSON.parse(r.data)) {
        const vid = a[5];
        if (!vid || !POSITIONS.has(a[1])) continue;
        total.set(vid, (total.get(vid) || 0) + 5);
        const cur = w.get(vid) || { callsign: a[0], minutes: 0 };
        cur.minutes += 5;
        cur.callsign = a[0];
        w.set(vid, cur);
      }
    }
    this.atcAgg = { at: Date.now(), total, weeks };
    return this.atcAgg;
  }

  /** Old tables from earlier versions: dropped once their data is past the retention window. */
  cleanupOld(minute) {
    try {
      const r = this.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('snap', 'active', 'atc_week')").toArray();
      for (const t of r) {
        if (t.name === 'snap') {
          const mx = this.sql.exec('SELECT MAX(minute) AS m FROM snap WHERE shard = 0').toArray()[0];
          if (mx && mx.m && mx.m > minute - KEEP_MIN) continue; // still has useful history
        }
        this.sql.exec(`DROP TABLE ${t.name}`);
      }
    } catch (e) {
      this.setMeta({ cleanupError: String(e && e.message ? e.message : e) });
    }
  }

  /** Positions of one IVAO session, oldest first, in the same shape as IVAO's tracks endpoint. */
  track(sessionId, sinceMs) {
    const id = sessionId >>> 0;
    const now = Math.floor(Date.now() / 60000);
    const from = Math.max(now - KEEP_MIN, sinceMs ? Math.floor(sinceMs / 60000) + 1 : 0);
    const shard = id % SHARDS;
    const m = this.shardMap(shard);
    const rows = [...m.keys()].filter((k) => k >= from).sort((a, b) => a - b).map((k) => ({ minute: k, data: m.get(k) }));
    // Switching from the old storage: history from before the ring existed is still in `snap`.
    const oldest = rows.length ? rows[0].minute : Infinity;
    if (oldest > from && !this.noOldSnap) {
      try {
        const old = this.sql.exec('SELECT minute, data FROM snap WHERE shard = ? AND minute >= ? AND minute < ? ORDER BY minute', shard, from, oldest).toArray();
        rows.unshift(...old.map((r) => ({ minute: r.minute, data: toU8(r.data) })));
      } catch {
        this.noOldSnap = true; // table gone
      }
    }
    const pts = [];
    for (const r of rows) {
      const dv = new DataView(r.data.buffer, r.data.byteOffset, r.data.byteLength);
      const count = Math.floor(dv.byteLength / REC);
      for (let i = 0; i < count; i++) {
        const o = i * REC;
        if (dv.getUint32(o) !== id) continue;
        pts.push({
          latitude: Math.round(dv.getFloat32(o + 4) * 1e5) / 1e5,
          longitude: Math.round(dv.getFloat32(o + 8) * 1e5) / 1e5,
          altitude: dv.getUint16(o + 12) * 2,
          groundSpeed: dv.getUint16(o + 14),
          onGround: dv.getUint8(o + 16) === 1,
          timestamp: new Date(r.minute * 60000).toISOString(),
        });
        break;
      }
    }
    return pts;
  }
}

// ───────── ATC clearances

const POSITIONS = new Set(['DEL', 'GND', 'TWR', 'APP', 'DEP', 'CTR', 'FSS', 'RMP']);
const AIRPORT_POS = new Set(['DEL', 'GND', 'TWR', 'APP', 'DEP', 'RMP']);
const CS = /^[A-Z0-9_-]{3,16}$/;
const clean = (s, max) => String(s ?? '').replace(/[^\x20-\x7E]/g, '').trim().slice(0, max);
const randomCode = () => {
  const a = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const b = crypto.getRandomValues(new Uint8Array(4));
  return 'PU-' + Array.from(b, (x) => a[x % a.length]).join('');
};
const randomToken = () => Array.from(crypto.getRandomValues(new Uint8Array(24)), (x) => x.toString(16).padStart(2, '0')).join('');

Object.assign(Recorder.prototype, {
  /** Latest Whazzup snapshot, refetched when older than maxAge. */
  async getFeed(maxAge = 20000) {
    if (this.feed && Date.now() - this.feedAt < maxAge) return this.feed;
    const res = await fetch((this.env && this.env.WHAZZUP_URL) || WHAZZUP, { headers: { Accept: 'application/json', 'User-Agent': 'IVAO-Pulse-recorder/1.0 (+community)' } });
    if (!res.ok) throw new Error(`IVAO feed HTTP ${res.status}`);
    this.feed = await res.json();
    this.feedAt = Date.now();
    return this.feed;
  },
  atcList(feed) {
    return ((feed && feed.clients && feed.clients.atcs) || []).map((a) => ({
      id: a.id,
      vid: a.userId,
      callsign: String(a.callsign || '').toUpperCase(),
      position: String((a.atcSession && a.atcSession.position) || String(a.callsign || '').split('_').pop() || '').toUpperCase(),
      frequency: a.atcSession && a.atcSession.frequency,
      atis: ((a.atis && a.atis.lines) || []).join(' ').toUpperCase(),
    }));
  },
  pilotList(feed) {
    return ((feed && feed.clients && feed.clients.pilots) || []).map((p) => ({
      id: p.id,
      callsign: String(p.callsign || '').toUpperCase(),
      dep: p.flightPlan && p.flightPlan.departureId,
      arr: p.flightPlan && p.flightPlan.arrivalId,
    }));
  },

  /** Step 1: the controller says which station they are; we hand out a code for their ATIS. */
  async atcStart(body) {
    const callsign = clean(body.callsign, 16).toUpperCase();
    const vid = Number(body.vid);
    if (!CS.test(callsign) || !Number.isInteger(vid) || vid <= 0) return { status: 400, body: { error: 'Enter your VID and pick your station.' } };
    const feed = await this.getFeed();
    const atc = this.atcList(feed).find((a) => a.callsign === callsign);
    if (!atc) return { status: 404, body: { error: `${callsign} is not online on IVAO right now.` } };
    if (atc.vid !== vid) return { status: 403, body: { error: `${callsign} is connected under a different VID.` } };
    const code = randomCode();
    this.sql.exec('INSERT OR REPLACE INTO atc_pending (callsign, vid, code, expires) VALUES (?, ?, ?, ?)', callsign, vid, code, Date.now() + 20 * 60e3);
    return { status: 200, body: { code, callsign, position: atc.position } };
  },

  /** Step 2: we look for the code in that station's ATIS on the public feed. */
  async atcVerify(body) {
    const callsign = clean(body.callsign, 16).toUpperCase();
    const row = this.sql.exec('SELECT vid, code, expires FROM atc_pending WHERE callsign = ?', callsign).toArray()[0];
    if (!row || row.expires < Date.now()) return { status: 410, body: { error: 'Code expired. Start again to get a new one.' } };
    const feed = await this.getFeed(10000);
    const atc = this.atcList(feed).find((a) => a.callsign === callsign);
    if (!atc || atc.vid !== row.vid) return { status: 404, body: { error: `${callsign} is no longer online under that VID.` } };
    if (!atc.atis.includes(row.code)) return { status: 409, body: { error: `Code ${row.code} isn't in ${callsign}'s ATIS yet. IVAO's feed updates about every 15 s.`, waiting: true } };
    if (!POSITIONS.has(atc.position)) return { status: 403, body: { error: 'Only DEL, GND, TWR, APP, DEP, CTR and FSS stations can send clearances.' } };
    const token = randomToken();
    this.sql.exec('INSERT INTO atc_session (token, callsign, vid, atc_id, position, created) VALUES (?, ?, ?, ?, ?, ?)', token, callsign, row.vid, atc.id, atc.position, Date.now());
    this.sql.exec('DELETE FROM atc_pending WHERE callsign = ?', callsign);
    return { status: 200, body: { token, callsign, position: atc.position, frequency: atc.frequency } };
  },

  /** A verified session stays valid only while that exact ATC connection is online. */
  async atcAuth(token) {
    if (typeof token !== 'string' || !/^[0-9a-f]{48}$/.test(token)) return null;
    const s = this.sql.exec('SELECT callsign, vid, atc_id, position FROM atc_session WHERE token = ?', token).toArray()[0];
    if (!s) return null;
    const atc = this.atcList(await this.getFeed()).find((a) => a.callsign === s.callsign);
    if (!atc || atc.id !== s.atc_id || atc.vid !== s.vid) {
      this.sql.exec('DELETE FROM atc_session WHERE token = ?', token);
      return null;
    }
    return s;
  },

  async atcMe(token) {
    const s = await this.atcAuth(token);
    if (!s) return { status: 401, body: { error: 'Not verified, or your ATC connection ended.' } };
    const sent = this.sql.exec('SELECT pilot, data, at, ack FROM clearance WHERE from_cs = ? ORDER BY at DESC LIMIT 30', s.callsign).toArray().map((r) => ({ pilot: r.pilot, at: r.at, ack: r.ack || null, ...JSON.parse(r.data) }));
    return { status: 200, body: { callsign: s.callsign, position: s.position, sent } };
  },

  /** Step 3: send a squawk and/or taxi route to a pilot in your area. */
  async atcSend(body) {
    const s = await this.atcAuth(body.token);
    if (!s) return { status: 401, body: { error: 'Not verified, or your ATC connection ended.' } };
    const now = Date.now();
    const log = (this.sendLog.get(s.callsign) || []).filter((t) => now - t < 60e3);
    if (log.length >= 20) return { status: 429, body: { error: 'Too many clearances in a minute. Slow down a little.' } };
    const pilotCs = clean(body.pilot, 12).toUpperCase();
    const pilot = this.pilotList(await this.getFeed()).find((p) => p.callsign === pilotCs);
    if (!pilot) return { status: 404, body: { error: `${pilotCs} is not online.` } };
    const icao = s.callsign.split('_')[0];
    if (AIRPORT_POS.has(s.position) && pilot.dep !== icao && pilot.arr !== icao) return { status: 403, body: { error: `${pilotCs} isn't departing from or arriving at ${icao}.` } };
    const out = {};
    if (body.squawk != null && body.squawk !== '') {
      const sq = clean(body.squawk, 4);
      if (!/^[0-7]{4}$/.test(sq)) return { status: 400, body: { error: 'A squawk is 4 digits from 0 to 7.' } };
      out.squawk = sq;
    }
    if (body.taxi) {
      if (s.position === 'CTR' || s.position === 'FSS') return { status: 403, body: { error: 'Taxi clearances come from DEL, GND, TWR or ramp.' } };
      const kind = body.taxi.kind === 'stand' ? 'stand' : 'runway';
      const dest = clean(body.taxi.dest, 6).toUpperCase();
      const via = (Array.isArray(body.taxi.via) ? body.taxi.via : []).slice(0, 10).map((v) => clean(v, 5).toUpperCase()).filter((v) => /^[A-Z0-9]{1,5}$/.test(v));
      const hold = clean(body.taxi.hold, 5).toUpperCase();
      if (!/^[A-Z0-9]{1,6}$/.test(dest)) return { status: 400, body: { error: 'Pick a runway or stand.' } };
      out.taxi = { kind, dest, via, ...(hold && /^[A-Z0-9]{1,5}$/.test(hold) ? { hold } : {}), airport: icao };
    }
    const note = clean(body.note, 90);
    if (note) out.note = note;
    if (!out.squawk && !out.taxi && !out.note) return { status: 400, body: { error: 'Add a squawk, a taxi route or a note.' } };
    out.from = s.callsign;
    out.position = s.position;
    this.sql.exec('INSERT OR REPLACE INTO clearance (pilot, session, data, at, ack, from_cs) VALUES (?, ?, ?, ?, NULL, ?)', pilotCs, pilot.id, JSON.stringify(out), now, s.callsign);
    log.push(now);
    this.sendLog.set(s.callsign, log);
    return { status: 200, body: { ok: true, pilot: pilotCs, at: now } };
  },

  /** The pilot's Pulse asks for the latest clearance for its callsign and session. */
  clearanceFor(callsign, session) {
    const cs = clean(callsign, 12).toUpperCase();
    const r = this.sql.exec('SELECT session, data, at, ack FROM clearance WHERE pilot = ?', cs).toArray()[0];
    if (!r || (session && r.session !== session)) return { status: 200, body: null };
    return { status: 200, body: { at: r.at, ack: r.ack || null, ...JSON.parse(r.data) } };
  },

  clearanceAck(callsign, body) {
    const cs = clean(callsign, 12).toUpperCase();
    const at = Number(body.at);
    const r = this.sql.exec('SELECT at FROM clearance WHERE pilot = ?', cs).toArray()[0];
    if (!r || r.at !== at) return { status: 409, body: { error: 'That clearance was replaced by a newer one.' } };
    this.sql.exec('UPDATE clearance SET ack = ? WHERE pilot = ?', Date.now(), cs);
    return { status: 200, body: { ok: true } };
  },
});

const recorder = (env) => env.REC.get(env.REC.idFromName('main'));
function toU8(d) {
  return d instanceof Uint8Array ? d : d instanceof ArrayBuffer ? new Uint8Array(d) : new Uint8Array(d.buffer || d);
}

function b64(u8) {
  let s = '';
  for (let i = 0; i < u8.length; i += 0x8000) s += String.fromCharCode.apply(null, u8.subarray(i, i + 0x8000));
  return btoa(s);
}

/** Weeks since the epoch, starting on Mondays (UTC). */
function weekOf(ms) {
  return Math.floor((Math.floor(ms / 864e5) + 3) / 7);
}

export default {
  async scheduled(_event, env, ctx) {
    ctx.waitUntil(recorder(env).record());
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });

    // ATC clearances
    const rec = recorder(env);
    const body = async () => {
      const t = await request.text();
      if (t.length > 4000) throw new Error('too large');
      return JSON.parse(t || '{}');
    };
    const reply = (r) => json(r.body, r.status, { 'Cache-Control': 'no-store' });
    try {
      if (request.method === 'POST' && url.pathname === '/atc/start') return reply(await rec.atcStart(await body()));
      if (request.method === 'POST' && url.pathname === '/atc/verify') return reply(await rec.atcVerify(await body()));
      if (request.method === 'POST' && url.pathname === '/atc/me') return reply(await rec.atcMe((await body()).token));
      if (request.method === 'POST' && url.pathname === '/atc/send') return reply(await rec.atcSend(await body()));
      if (request.method === 'POST' && url.pathname === '/touchdown') return reply(await rec.touchdown(await body()));
      const c = /^\/clearance\/([A-Za-z0-9_-]{2,12})(\/ack)?\/?$/.exec(url.pathname);
      if (c && request.method === 'GET' && !c[2]) return reply(await rec.clearanceFor(c[1], Number(url.searchParams.get('session')) || 0));
      if (c && request.method === 'POST' && c[2]) return reply(await rec.clearanceAck(c[1], await body()));
    } catch (e) {
      return json({ error: 'Bad request' }, 400);
    }
    if (request.method !== 'GET') return json({ error: 'Not found' }, 404);

    const m = /^\/tracks\/(\d{1,10})\/?$/.exec(url.pathname);
    if (m) {
      const since = Number(url.searchParams.get('since')) || 0;
      // Short edge cache: many viewers of the same flight cost one Durable Object read.
      const cache = caches.default;
      const key = new Request(`${url.origin}/tracks/${m[1]}?since=${since ? Math.floor(since / 60000) : 0}`);
      const hit = await cache.match(key);
      if (hit) return hit;
      const pts = await recorder(env).track(Number(m[1]), since);
      // Only cache a real track: an empty answer must not hide history that arrives a minute later.
      const res = json(pts, 200, { 'Cache-Control': pts.length >= 2 ? 'public, max-age=50' : 'no-store' });
      if (pts.length >= 2) ctx.waitUntil(cache.put(key, res.clone()));
      return res;
    }
    const lb = /^\/logbook\/(\d{3,8})\/?$/.exec(url.pathname);
    if (lb) return json(await rec.logbookFor(Number(lb[1])), 200, { 'Cache-Control': 'no-store' });
    if (url.pathname === '/replay') {
      const now = Math.floor(Date.now() / 60000);
      const from = Math.floor(Number(url.searchParams.get('from')) || now - 30);
      const n = Math.max(1, Math.min(15, Math.floor(Number(url.searchParams.get('n')) || 10)));
      if (from < now - KEEP_MIN - 5 || from > now) return json({ error: 'Out of range' }, 400);
      const cache = caches.default;
      const key = new Request(`${url.origin}/replay?from=${from}&n=${n}`);
      const hit = await cache.match(key);
      if (hit) return hit;
      const body = await rec.replay(from, n);
      const complete = from + n <= (body.latest || 0) && body.frames.length > 0; // never cache an empty window
      const res = json(body, 200, { 'Cache-Control': complete ? 'public, max-age=3600' : 'no-store' });
      if (complete) ctx.waitUntil(cache.put(key, res.clone()));
      return res;
    }
    if (url.pathname === '/leaderboard') return json(await rec.leaderboard(url.searchParams.get('week')), 200, { 'Cache-Control': 'public, max-age=120' });
    if (url.pathname === '/stats') return json(await rec.stats(), 200, { 'Cache-Control': 'public, max-age=120' });
    if (url.pathname === '/' || url.pathname === '/status') {
      return json({ service: 'IVAO Pulse track recorder', ...(await recorder(env).status()) });
    }
    return json({ error: 'Not found' }, 404);
  },
};
