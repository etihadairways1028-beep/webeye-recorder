// IVAO WebEye track recorder.
//
// Every minute a cron trigger asks one SQLite-backed Durable Object to fetch IVAO's public
// Whazzup feed and save every connected pilot's position. The map then asks
// GET /tracks/<sessionId> for a flight's full track from pushback. No API key is involved:
// it only reads the same public feed the map uses.
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
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
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

const recorder = (env) => env.REC.get(env.REC.idFromName('main'));

export default {
  async scheduled(_event, env, ctx) {
    ctx.waitUntil(recorder(env).record());
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
    if (request.method !== 'GET') return json({ error: 'GET only' }, 405);

    const m = /^\/tracks\/(\d{1,10})\/?$/.exec(url.pathname);
    if (m) {
      const since = Number(url.searchParams.get('since')) || 0;
      // Short edge cache: many viewers of the same flight cost one Durable Object read.
      const cache = caches.default;
      const key = new Request(`${url.origin}/tracks/${m[1]}?since=${since ? Math.floor(since / 60000) : 0}`);
      const hit = await cache.match(key);
      if (hit) return hit;
      const pts = await recorder(env).track(Number(m[1]), since);
      const res = json(pts, 200, { 'Cache-Control': 'public, max-age=50' });
      ctx.waitUntil(cache.put(key, res.clone()));
      return res;
    }
    if (url.pathname === '/' || url.pathname === '/status') {
      return json({ service: 'IVAO WebEye track recorder', ...(await recorder(env).status()) });
    }
    return json({ error: 'Not found' }, 404);
  },
};
