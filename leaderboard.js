'use strict';
/**
 * Global ranking ("smartest players"): who gave the most correct answers.
 *
 *   POST /lb/submit   {id, name, country, avatar, level, correct}
 *   GET  /lb/top?scope=world|country&country=MA&limit=50&id=<my id>
 *   POST /lb/remove   {id}
 *   POST /lb/report   {id: reporter, ref: target, reason: 'name'|'other'}
 *   GET  /lb/admin?key=<ADMIN_KEY>      (your moderation page, see DEPLOY_AR.md)
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
const crypto = require('crypto');
const { isNameAllowed } = require('./moderation');

const ID_RE = /^[a-f0-9]{24}$/;
const COUNTRY_RE = /^[A-Z]{2}$/;
const MAX_BODY = 2048;
const MAX_SCORE = 5000000;
const FIRST_SUBMIT_MAX = 20000; // a player may arrive with lots of earlier progress
const MAX_PER_MIN = 40; // sustained correct answers per minute that we believe
const RATE_PER_MIN = 90; // requests per IP per minute
const REPORT_HIDE = 3; // distinct reporters (ids AND networks) that hide a player
const REF_RE = /^[a-f0-9]{12}$/;
const SALT = process.env.LB_SALT || 'twm-default-salt';

const sha = (x) => crypto.createHash('sha256').update(x).digest('hex');
/** Public, non-reversible handle of a player (the real id stays secret). */
const refOf = (id) => sha('ref:' + id + SALT).slice(0, 12);
const ipHashOf = (ip) => sha('ip:' + ip + SALT).slice(0, 16);

// ───────────────────────── stores ─────────────────────────

class MemoryStore {
  constructor(file) {
    this.kind = 'file';
    this.file = file;
    this.players = new Map();
    this.reports = new Map(); // target id → {ids:Set, ips:Set}
    this.dirty = false;
    this.cache = new Map(); // scope key → {at, list}
    try {
      const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
      for (const p of raw.players || []) this.players.set(p.id, p);
      for (const r of raw.reports || []) this.reports.set(r.id, { ids: new Set(r.ids), ips: new Set(r.ips) });
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
      const reports = [...this.reports.entries()].map(([id, r]) => ({ id, ids: [...r.ids], ips: [...r.ips] }));
      fs.writeFileSync(tmp, JSON.stringify({ players: [...this.players.values()], reports }));
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
    const had = this.players.delete(id);
    this.reports.delete(id);
    if (had) {
      this.dirty = true;
      this.cache.clear();
    }
  }
  async getByRef(ref) {
    for (const p of this.players.values()) if (p.ref === ref) return p;
    return null;
  }
  async setHidden(id, hidden) {
    const p = this.players.get(id);
    if (!p) return;
    p.hidden = !!hidden;
    this.dirty = true;
    this.cache.clear();
  }
  async addReport(target, reporter, ipHash) {
    let r = this.reports.get(target);
    if (!r) this.reports.set(target, (r = { ids: new Set(), ips: new Set() }));
    r.ids.add(reporter);
    r.ips.add(ipHash);
    this.dirty = true;
    return { ids: r.ids.size, ips: r.ips.size };
  }
  async listReported() {
    const out = [];
    for (const [id, r] of this.reports) {
      const p = this.players.get(id);
      if (p) out.push({ player: p, ids: r.ids.size, ips: r.ips.size });
    }
    return out;
  }
  _sorted(scope, country) {
    const key = scope === 'country' ? 'c:' + country : 'w';
    const hit = this.cache.get(key);
    if (hit && Date.now() - hit.at < 4000) return hit.list;
    let list = [...this.players.values()].filter((p) => p.correct > 0 && !p.hidden);
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
      ['SET', 'r:' + p.ref, p.id],
    ];
    if (old && old.country && old.country !== p.country) cmds.push(['ZREM', 'lb:c:' + old.country, p.id]);
    if (p.hidden) {
      cmds.push(['ZREM', 'lb:world', p.id]);
      if (p.country) cmds.push(['ZREM', 'lb:c:' + p.country, p.id]);
    } else {
      cmds.push(['ZADD', 'lb:world', p.correct, p.id]);
      if (p.country) cmds.push(['ZADD', 'lb:c:' + p.country, p.correct, p.id]);
    }
    await this.pipe(cmds);
  }
  async remove(id) {
    const old = await this.get(id);
    const cmds = [
      ['DEL', 'p:' + id],
      ['ZREM', 'lb:world', id],
      ['DEL', 'rep:' + id],
      ['DEL', 'repip:' + id],
      ['SREM', 'rep:index', id],
    ];
    if (old && old.country) cmds.push(['ZREM', 'lb:c:' + old.country, id]);
    if (old && old.ref) cmds.push(['DEL', 'r:' + old.ref]);
    await this.pipe(cmds);
  }
  async getByRef(ref) {
    const [id] = await this.pipe([['GET', 'r:' + ref]]);
    return id ? this.get(id) : null;
  }
  async setHidden(id, hidden) {
    const p = await this.get(id);
    if (!p) return;
    p.hidden = !!hidden;
    await this.put(p);
  }
  async addReport(target, reporter, ipHash) {
    const r = await this.pipe([
      ['SADD', 'rep:' + target, reporter],
      ['SADD', 'repip:' + target, ipHash],
      ['SADD', 'rep:index', target],
      ['SCARD', 'rep:' + target],
      ['SCARD', 'repip:' + target],
    ]);
    return { ids: Number(r[3]) || 0, ips: Number(r[4]) || 0 };
  }
  async listReported() {
    const [ids] = await this.pipe([['SMEMBERS', 'rep:index']]);
    if (!ids || !ids.length) return [];
    const cmds = [];
    for (const id of ids) cmds.push(['GET', 'p:' + id], ['SCARD', 'rep:' + id], ['SCARD', 'repip:' + id]);
    const r = await this.pipe(cmds);
    const out = [];
    for (let i = 0; i < ids.length; i++) {
      if (!r[i * 3]) continue;
      try {
        out.push({ player: JSON.parse(r[i * 3]), ids: Number(r[i * 3 + 1]) || 0, ips: Number(r[i * 3 + 2]) || 0 });
      } catch (_) {}
    }
    return out;
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

// ───────────────────────── admin page ─────────────────────────

const esc = (v) =>
  String(v).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);

function keyOk(given) {
  const want = process.env.ADMIN_KEY || '';
  if (want.length < 8) return false; // admin disabled unless a real key is set
  const a = crypto.createHash('sha256').update(String(given || '')).digest();
  const b = crypto.createHash('sha256').update(want).digest();
  return crypto.timingSafeEqual(a, b);
}

function html(res, code, body) {
  res.writeHead(code, {
    'Content-Type': 'text/html; charset=utf-8',
    'Cache-Control': 'no-store',
    'X-Robots-Tag': 'noindex',
  });
  res.end(body);
}

async function adminPage(store, url, res) {
  const key = url.searchParams.get('key') || '';
  if (!keyOk(key)) {
    reply(res, 404, { ok: false, error: 'not_found' });
    return;
  }
  const act = url.searchParams.get('act') || '';
  const ref = url.searchParams.get('ref') || '';
  let msg = '';
  if ((act === 'hide' || act === 'restore') && REF_RE.test(ref)) {
    const p = await store.getByRef(ref);
    if (p) {
      await store.setHidden(p.id, act === 'hide');
      msg = (act === 'hide' ? 'Hidden: ' : 'Restored: ') + p.name;
    }
  }
  const rows = (await store.listReported()).sort((x, y) => y.ids - x.ids || y.player.correct - x.player.correct);
  const k = encodeURIComponent(key);
  const trs = rows
    .map((r) => {
      const p = r.player;
      const ref12 = p.ref || refOf(p.id);
      const btn = p.hidden
        ? `<a class="b ok" href="?key=${k}&act=restore&ref=${ref12}">Restore</a>`
        : `<a class="b no" href="?key=${k}&act=hide&ref=${ref12}">Hide</a>`;
      return `<tr><td>${esc(p.name)}</td><td>${esc(p.country || '-')}</td><td>${p.correct}</td><td>${r.ids}/${r.ips}</td><td>${
        p.hidden ? 'hidden' : 'visible'
      }</td><td>${btn}</td></tr>`;
    })
    .join('');
  html(
    res,
    200,
    `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Moderation</title><style>
body{font-family:system-ui,sans-serif;background:#0b1236;color:#fff;margin:0;padding:16px}
h1{font-size:20px} table{width:100%;border-collapse:collapse;font-size:14px}
td,th{padding:8px;border-bottom:1px solid #2a3570;text-align:left} .b{padding:6px 12px;border-radius:8px;text-decoration:none;color:#fff;font-weight:700}
.no{background:#c4162c}.ok{background:#12a85c} .m{background:#243a8a;padding:8px 12px;border-radius:8px;margin-bottom:12px}
</style></head><body><h1>Reported players</h1>${msg ? `<div class="m">${esc(msg)}</div>` : ''}
<p>Columns: name, country, score, reports (people / networks), status. A player is hidden automatically after ${REPORT_HIDE} reports from different people on different networks.</p>
<table><tr><th>Name</th><th>Country</th><th>Score</th><th>Reports</th><th>Status</th><th></th></tr>${
      trs || '<tr><td colspan="6">No reports yet.</td></tr>'
    }</table></body></html>`
  );
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
        let name = cleanName(b.name);
        const nameOk = isNameAllowed(name);
        if (!nameOk) name = 'Player';
        const p = {
          id: b.id,
          ref: refOf(b.id),
          name,
          country,
          avatar: toInt(b.avatar, 0, 9, 4),
          level: toInt(b.level, 1, 100000, 1),
          correct: sanitizeScore(old, wanted, now),
          ts: now,
          hidden: !!(old && old.hidden),
        };
        await store.put(p);
        const world = await store.rankOf(p.id, 'world', '');
        const mine = country ? await store.rankOf(p.id, 'country', country) : { rank: null, total: 0 };
        reply(res, 200, { ok: true, correct: p.correct, nameOk, world, country: mine });
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

      if (url.pathname === '/lb/report' && req.method === 'POST') {
        let b;
        try {
          b = JSON.parse(await readBody(req));
        } catch (e) {
          reply(res, e && e.message === 'too big' ? 413 : 400, { ok: false, error: 'bad' });
          return true;
        }
        if (!b || typeof b.id !== 'string' || !ID_RE.test(b.id) || typeof b.ref !== 'string' || !REF_RE.test(b.ref)) {
          reply(res, 400, { ok: false, error: 'bad' });
          return true;
        }
        const reporter = await store.get(b.id); // only real players can report
        if (!reporter) {
          reply(res, 403, { ok: false, error: 'unknown_reporter' });
          return true;
        }
        const target = await store.getByRef(b.ref);
        if (!target) {
          reply(res, 404, { ok: false, error: 'not_found' });
          return true;
        }
        if (target.id !== b.id) {
          const c = await store.addReport(target.id, b.id, ipHashOf(ip));
          if (!target.hidden && Math.min(c.ids, c.ips) >= REPORT_HIDE) await store.setHidden(target.id, true);
        }
        reply(res, 200, { ok: true });
        return true;
      }

      if (url.pathname === '/lb/admin' && req.method === 'GET') {
        await adminPage(store, url, res);
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
          ref: p.ref || refOf(p.id),
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
              if (p) me = { rank: r.rank, ref: p.ref || refOf(p.id), name: p.name, country: p.country, avatar: p.avatar, level: p.level, correct: p.correct, me: true };
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
