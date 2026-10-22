// IVAO WebEye track recorder.
//
// Every minute a cron trigger asks one SQLite-backed Durable Object to fetch IVAO's public
// Whazzup feed and save every connected pilot's position. The map then asks
// GET /tracks/<sessionId> for a flight's full track from pushback. No API key is involved:
// it only reads the same public feed the map uses.
//
// It also carries ATC clearances: a controller proves which station they are by putting a
// one-time code in their ATIS (read back from the public feed), then sends squawks and taxi
// routes that the pilot's WebEye picks up. See "ATC clearances" below.
//
// Free-plan budget (per day): ~23k rows inserted + ~23k deleted (limit 100k written),
// track reads use the (shard, minute) key so a long-haul track costs ≤ 1,080 rows read.
import { DurableObject } from 'cloudflare:workers';

const WHAZZUP = 'https://api.ivao.aero/v2/tracker/whazzup';
const SHARDS = 16; // pilots are spread over 16 rows per minute
const KEEP_MIN = 18 * 60; // keep 18 hours: enough for the longest flights
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
    this.sql.exec(`CREATE TABLE IF NOT EXISTS snap (shard INTEGER NOT NULL, minute INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (shard, minute)) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID`);
    // ATC: pending verifications, verified sessions, and the latest clearance per pilot.
    this.sql.exec(`CREATE TABLE IF NOT EXISTS atc_pending (callsign TEXT PRIMARY KEY, vid INTEGER, code TEXT, expires INTEGER) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS atc_session (token TEXT PRIMARY KEY, callsign TEXT, vid INTEGER, atc_id INTEGER, position TEXT, created INTEGER) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS clearance (pilot TEXT PRIMARY KEY, session INTEGER, data TEXT, at INTEGER, ack INTEGER, from_cs TEXT) WITHOUT ROWID`);
    this.feed = null;
    this.feedAt = 0;
    this.sendLog = new Map();
  }

  /** Called by the cron trigger once a minute. */
  async record() {
    // TEST_FAKE_MINUTES (local testing only): each run counts as the next minute.
    const minute = this.env && this.env.TEST_FAKE_MINUTES ? Math.floor(Date.now() / 60000) - 60 + (this.fake = (this.fake || 0) + 1) : Math.floor(Date.now() / 60000);
    let pilots;
    try {
      const res = await fetch((this.env && this.env.WHAZZUP_URL) || WHAZZUP, { headers: { Accept: 'application/json', 'User-Agent': 'IVAO-WebEye-recorder/1.0 (+community)' } });
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
      this.sql.exec('INSERT OR REPLACE INTO snap (shard, minute, data) VALUES (?, ?, ?)', s, minute, buf);
      n += list.length;
    }

    // Prune once every 10 minutes, shard by shard along the key (cheap on rows read).
    if (minute % 10 === 0) {
      const cutoff = minute - KEEP_MIN;
      for (let s = 0; s < SHARDS; s++) this.sql.exec('DELETE FROM snap WHERE shard = ? AND minute < ?', s, cutoff);
      const now = Date.now();
      this.sql.exec('DELETE FROM atc_pending WHERE expires < ?', now);
      this.sql.exec('DELETE FROM atc_session WHERE created < ?', now - 24 * 3600e3);
      this.sql.exec('DELETE FROM clearance WHERE at < ?', now - 6 * 3600e3);
    }
    this.setMeta({ lastMinute: String(minute), pilots: String(n) });
    return { ok: true, pilots: n };
  }

  setMeta(obj) {
    for (const [k, v] of Object.entries(obj)) this.sql.exec('INSERT OR REPLACE INTO meta (k, v) VALUES (?, ?)', k, v);
  }

  status() {
    const out = {};
    for (const r of this.sql.exec('SELECT k, v FROM meta').toArray()) out[r.k] = r.v;
    if (out.lastMinute) out.lastRecorded = new Date(Number(out.lastMinute) * 60000).toISOString();
    return out;
  }

  /** Positions of one IVAO session, oldest first, in the same shape as IVAO's tracks endpoint. */
  track(sessionId, sinceMs) {
    const id = sessionId >>> 0;
    const now = Math.floor(Date.now() / 60000);
    const from = Math.max(now - KEEP_MIN, sinceMs ? Math.floor(sinceMs / 60000) + 1 : 0);
    const rows = this.sql.exec('SELECT minute, data FROM snap WHERE shard = ? AND minute >= ? ORDER BY minute', id % SHARDS, from).toArray();
    const pts = [];
    for (const r of rows) {
      const dv = new DataView(r.data instanceof ArrayBuffer ? r.data : new Uint8Array(r.data).buffer);
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
  return 'WE-' + Array.from(b, (x) => a[x % a.length]).join('');
};
const randomToken = () => Array.from(crypto.getRandomValues(new Uint8Array(24)), (x) => x.toString(16).padStart(2, '0')).join('');

Object.assign(Recorder.prototype, {
  /** Latest Whazzup snapshot, refetched when older than maxAge. */
  async getFeed(maxAge = 20000) {
    if (this.feed && Date.now() - this.feedAt < maxAge) return this.feed;
    const res = await fetch((this.env && this.env.WHAZZUP_URL) || WHAZZUP, { headers: { Accept: 'application/json', 'User-Agent': 'IVAO-WebEye-recorder/1.0 (+community)' } });
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

  /** The pilot's WebEye asks for the latest clearance for its callsign and session. */
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
    if (url.pathname === '/' || url.pathname === '/status') {
      return json({ service: 'IVAO WebEye track recorder', ...(await recorder(env).status()) });
    }
    return json({ error: 'Not found' }, 404);
  },
};
