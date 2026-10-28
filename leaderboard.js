'use strict';
/**
 * Global ranking ("smartest players"): who gave the most correct answers.
 *
 *   POST /lb/submit   {id, name, country, avatar, level, correct}
 *   GET  /lb/top?scope=world|country&country=MA&limit=50&id=<my id>
 *   POST /lb/remove   {id}
 *
 * Storage (chosen automatically):
 *   • Upstash Redis (free) when UPSTASH_REDIS_REST_URL and
 *     UPSTASH_REDIS_REST_TOKEN are set  → the ranking survives restarts, sleep
 *     and redeploys. This is what you want on a free hosting plan.
 *   • Otherwise a JSON file (LB_FILE, default ./data/leaderboard.json). Fine
 *     for a PC or a host with a persistent disk, but a free Render instance
 *     loses it when it restarts.
 *
 * The app is trusted only loosely: the score can only go up, and it cannot go
 * up faster than a human can answer (see sanitizeScore).
 */
const fs = require('fs');
const path = require('path');

const ID_RE = /^[a-f0-9]{24}$/;
const COUNTRY_RE = /^[A-Z]{2}$/;
const MAX_BODY = 2048;
const MAX_SCORE = 5000000;
const FIRST_SUBMIT_MAX = 20000; // a player may arrive with lots of earlier progress
const MAX_PER_MIN = 40; // sustained correct answers per minute that we believe
const RATE_PER_MIN = 90; // requests per IP per minute

// ───────────────────────── stores ─────────────────────────

class MemoryStore {
  constructor(file) {
    this.kind = 'file';
    this.file = file;
    this.players = new Map();
    this.dirty = false;
    this.cache = new Map(); // scope key → {at, list}
    try {
      const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
      for (const p of raw.players || []) this.players.set(p.id, p);
    } catch (_) {
      /* first run */
    }
    this.timer = setInterval(() => this.flush(), 15000);
    if (this.timer.unref) this.timer.unref();
  }
  flush() {
    if (!this.dirty) return;
    this.dirty = false;
    try {
      fs.mkdirSync(path.dirname(this.file), { recursive: true });
      const tmp = this.file + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify({ players: [...this.players.values()] }));
      fs.renameSync(tmp, this.file);
    } catch (_) {
      this.dirty = true;
    }
  }
  async get(id) {
    return this.players.get(id) || null;
  }
  async put(p) {
    this.players.set(p.id, p);
    this.dirty = true;
    this.cache.clear();
  }
  async remove(id) {
    if (this.players.delete(id)) {
      this.dirty = true;
      this.cache.clear();
    }
  }
  _sorted(scope, country) {
    const key = scope === 'country' ? 'c:' + country : 'w';
    const hit = this.cache.get(key);
    if (hit && Date.now() - hit.at < 4000) return hit.list;
    let list = [...this.players.values()].filter((p) => p.correct > 0);
    if (scope === 'country') list = list.filter((p) => p.country === country);
    // same tie-break as Redis: higher score first, then higher id
    list.sort((a, b) => b.correct - a.correct || (a.id < b.id ? 1 : a.id > b.id ? -1 : 0));
    this.cache.set(key, { at: Date.now(), list });
    return list;
  }
  async top(scope, country, limit) {
    return this._sorted(scope, country).slice(0, limit);
  }
  async rankOf(id, scope, country) {
    const list = this._sorted(scope, country);
    const i = list.findIndex((p) => p.id === id);
    return { rank: i < 0 ? null : i + 1, total: list.length };
  }
}

class UpstashStore {
  constructor(url, token) {
    this.kind = 'upstash';
    this.url = url.replace(/\/+$/, '');
    this.token = token;
  }
  async pipe(cmds) {
    const res = await fetch(this.url + '/pipeline', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + this.token, 'Content-Type': 'application/json' },
      body: JSON.stringify(cmds),
    });
    if (!res.ok) throw new Error('upstash http ' + res.status);
    const out = await res.json();
    if (!Array.isArray(out)) throw new Error('upstash bad reply');
    for (const r of out) if (r && r.error) throw new Error('upstash: ' + r.error);
    return out.map((r) => (r ? r.result : null));
  }
  async get(id) {
    const [raw] = await this.pipe([['GET', 'p:' + id]]);
    if (!raw) return null;
    try {
      return JSON.parse(raw);
    } catch (_) {
      return null;
    }
  }
  async put(p) {
    const old = await this.get(p.id);
    const cmds = [
      ['SET', 'p:' + p.id, JSON.stringify(p)],
      ['ZADD', 'lb:world', p.correct, p.id],
    ];
    if (old && old.country && old.country !== p.country) cmds.push(['ZREM', 'lb:c:' + old.country, p.id]);
    if (p.country) cmds.push(['ZADD', 'lb:c:' + p.country, p.correct, p.id]);
    await this.pipe(cmds);
  }
  async remove(id) {
    const old = await this.get(id);
    const cmds = [['DEL', 'p:' + id], ['ZREM', 'lb:world', id]];
    if (old && old.country) cmds.push(['ZREM', 'lb:c:' + old.country, id]);
    await this.pipe(cmds);
  }
  _key(scope, country) {
    return scope === 'country' ? 'lb:c:' + country : 'lb:world';
  }
  async top(scope, country, limit) {
    const [ids] = await this.pipe([['ZREVRANGE', this._key(scope, country), 0, limit - 1]]);
    if (!ids || !ids.length) return [];
    const [raws] = await this.pipe([['MGET', ...ids.map((i) => 'p:' + i)]]);
    const out = [];
    for (const r of raws || []) {
      if (!r) continue;
      try {
        out.push(JSON.parse(r));
      } catch (_) {}
    }
    return out;
  }
  async rankOf(id, scope, country) {
    const key = this._key(scope, country);
    const [rank, total] = await this.pipe([['ZREVRANK', key, id], ['ZCARD', key]]);
    return { rank: rank === null || rank === undefined ? null : Number(rank) + 1, total: Number(total) || 0 };
  }
}

function makeStore(env = process.env) {
  if (env.UPSTASH_REDIS_REST_URL && env.UPSTASH_REDIS_REST_TOKEN) {
    return new UpstashStore(env.UPSTASH_REDIS_REST_URL, env.UPSTASH_REDIS_REST_TOKEN);
  }
  return new MemoryStore(env.LB_FILE || path.join(__dirname, 'data', 'leaderboard.json'));
}

// ───────────────────────── rules ─────────────────────────

function cleanName(v) {
  let s = typeof v === 'string' ? v : '';
  s = s.replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim();
  return Array.from(s).slice(0, 16).join('') || 'Player';
}

function toInt(v, lo, hi, d) {
  const n = typeof v === 'number' && Number.isFinite(v) ? Math.trunc(v) : d;
  return Math.min(hi, Math.max(lo, n));
}

/** The score never goes down, and never climbs faster than a person can answer. */
function sanitizeScore(old, wanted, now) {
  if (!old) return Math.min(wanted, FIRST_SUBMIT_MAX);
  const minutes = Math.max(0, (now - old.ts) / 60000);
  const room = 300 + Math.floor(minutes * MAX_PER_MIN);
  return Math.max(old.correct, Math.min(wanted, old.correct + room));
}

const hits = new Map(); // ip → {start, n}
function limited(ip) {
  const now = Date.now();
  let h = hits.get(ip);
  if (!h || now - h.start > 60000) {
    h = { start: now, n: 0 };
    hits.set(ip, h);
  }
  h.n++;
  if (hits.size > 5000) {
    for (const [k, v] of hits) if (now - v.start > 60000) hits.delete(k);
  }
  return h.n > RATE_PER_MIN;
}

function reply(res, code, obj) {
  const h = { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' };
  if (code === 413) h.Connection = 'close';
  res.writeHead(code, h);
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    let dead = false;
    const parts = [];
    req.on('data', (c) => {
      if (dead) return;
      size += c.length;
      if (size > MAX_BODY) {
        dead = true;
        parts.length = 0;
        reject(new Error('too big'));
        return;
      }
      parts.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(parts).toString('utf8')));
    req.on('error', reject);
  });
}

// ───────────────────────── http ─────────────────────────

function createHandler(store) {
  return async function handle(req, res) {
    const url = new URL(req.url || '/', 'http://x');
    if (!url.pathname.startsWith('/lb/')) return false;
    try {
      const fwd = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
      const ip = fwd || (req.socket && req.socket.remoteAddress) || '?';
      if (limited(ip)) {
        reply(res, 429, { ok: false, error: 'slow_down' });
        return true;
      }

      if (url.pathname === '/lb/submit' && req.method === 'POST') {
        let b;
        try {
          b = JSON.parse(await readBody(req));
        } catch (e) {
          reply(res, e && e.message === 'too big' ? 413 : 400, { ok: false, error: 'bad' });
          return true;
        }
        if (!b || typeof b !== 'object' || typeof b.id !== 'string' || !ID_RE.test(b.id)) {
          reply(res, 400, { ok: false, error: 'bad_id' });
          return true;
        }
        const now = Date.now();
        const old = await store.get(b.id);
        const wanted = toInt(b.correct, 0, MAX_SCORE, 0);
        const country = typeof b.country === 'string' && COUNTRY_RE.test(b.country) ? b.country : '';
        const p = {
          id: b.id,
          name: cleanName(b.name),
          country,
          avatar: toInt(b.avatar, 0, 9, 4),
          level: toInt(b.level, 1, 100000, 1),
          correct: sanitizeScore(old, wanted, now),
          ts: now,
        };
        await store.put(p);
        const world = await store.rankOf(p.id, 'world', '');
        const mine = country ? await store.rankOf(p.id, 'country', country) : { rank: null, total: 0 };
        reply(res, 200, { ok: true, correct: p.correct, world, country: mine });
        return true;
      }

      if (url.pathname === '/lb/remove' && req.method === 'POST') {
        let b;
        try {
          b = JSON.parse(await readBody(req));
        } catch (_) {
          reply(res, 400, { ok: false, error: 'bad' });
          return true;
        }
        if (!b || typeof b.id !== 'string' || !ID_RE.test(b.id)) {
          reply(res, 400, { ok: false, error: 'bad_id' });
          return true;
        }
        await store.remove(b.id);
        reply(res, 200, { ok: true });
        return true;
      }

      if (url.pathname === '/lb/top' && req.method === 'GET') {
        const scope = url.searchParams.get('scope') === 'country' ? 'country' : 'world';
        const country = String(url.searchParams.get('country') || '').toUpperCase();
        if (scope === 'country' && !COUNTRY_RE.test(country)) {
          reply(res, 400, { ok: false, error: 'bad_country' });
          return true;
        }
        const limit = toInt(Number(url.searchParams.get('limit')), 1, 100, 50);
        const myId = String(url.searchParams.get('id') || '');
        const list = await store.top(scope, country, limit);
        const rows = list.map((p, i) => ({
          rank: i + 1,
          name: p.name,
          country: p.country,
          avatar: p.avatar,
          level: p.level,
          correct: p.correct,
          me: p.id === myId,
        }));
        let total = rows.length;
        let me = null;
        if (ID_RE.test(myId)) {
          const r = await store.rankOf(myId, scope, country);
          total = Math.max(total, r.total);
          if (r.rank !== null) {
            const mine = rows.find((x) => x.me);
            if (mine) me = mine;
            else {
              const p = await store.get(myId);
              if (p) me = { rank: r.rank, name: p.name, country: p.country, avatar: p.avatar, level: p.level, correct: p.correct, me: true };
            }
          }
        } else {
          const r = await store.rankOf('', scope, country);
          total = Math.max(total, r.total);
        }
        reply(res, 200, { ok: true, scope, country, total, list: rows, me });
        return true;
      }

      reply(res, 404, { ok: false, error: 'not_found' });
      return true;
    } catch (e) {
      reply(res, 500, { ok: false, error: 'server' });
      return true;
    }
  };
}

module.exports = { createHandler, makeStore, MemoryStore, UpstashStore, sanitizeScore, cleanName };
