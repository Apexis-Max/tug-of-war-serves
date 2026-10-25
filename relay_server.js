'use strict';
/**
 * Tug of War Masters — relay server.
 *
 * The game is peer-to-peer in spirit (the host phone is the referee), but two
 * phones can rarely open a direct connection (mobile networks, NAT, firewalls).
 * This server only introduces the two players and forwards their messages; it
 * does not look at the game, store anything or keep any account.
 *
 *   client → server                          server → client
 *   {op:'create'}                            {op:'created', room:'123456'}
 *   {op:'join', room:'123456'}               {op:'paired', role:'host'|'guest'}
 *   {op:'quick'}                             {op:'queued'}
 *   {op:'cancel'}                            {op:'error', code:'not_found'|'full'|'bad'}
 *   {op:'relay', data:{…}}                   {op:'relay', data:{…}}
 *   {op:'ping'}                              {op:'pong'}
 *                                            {op:'peer_left'}
 *
 * Run:   npm install && npm start            (PORT=8080 by default)
 */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const MAX_PAYLOAD = 16 * 1024; // a whole round of questions is ~3 KB
const HEARTBEAT_MS = 25000;
const ROOM_TTL_MS = 10 * 60 * 1000; // an unjoined room disappears after 10 minutes
const MAX_MSGS_PER_SEC = 40;

/** @type {Map<string, {host: any, guest: any|null, created: number}>} */
const rooms = new Map();
/** @type {any[]} sockets waiting for a random opponent */
let queue = [];

// Plain HTTP endpoint next to the WebSocket: hosting platforms use it as a
// health check, and the app calls it first to wake a sleeping (free-plan)
// server before opening the WebSocket.
const server = http.createServer((req, res) => {
  const path = (req.url || '/').split('?')[0];
  if (path === '/' || path === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end('{"ok":true,"service":"tug-of-war-relay"}');
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD });

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function newRoomCode() {
  for (let i = 0; i < 50; i++) {
    const code = String(Math.floor(100000 + Math.random() * 900000));
    if (!rooms.has(code)) return code;
  }
  return null;
}

function pair(roomCode, host, guest) {
  const room = { host, guest, created: Date.now() };
  rooms.set(roomCode, room);
  host.room = roomCode;
  guest.room = roomCode;
  host.role = 'host';
  guest.role = 'guest';
  host.peer = guest;
  guest.peer = host;
  send(host, { op: 'paired', role: 'host' });
  send(guest, { op: 'paired', role: 'guest' });
}

function leaveQueue(ws) {
  queue = queue.filter((s) => s !== ws);
}

function leaveRoom(ws) {
  leaveQueue(ws);
  const code = ws.room;
  if (!code) return;
  const room = rooms.get(code);
  ws.room = null;
  if (!room) return;
  const other = room.host === ws ? room.guest : room.host;
  rooms.delete(code);
  if (other) {
    other.room = null;
    other.peer = null;
    send(other, { op: 'peer_left' });
  }
  ws.peer = null;
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.room = null;
  ws.peer = null;
  ws.role = null;
  ws.windowStart = Date.now();
  ws.windowCount = 0;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (raw) => {
    // light flood protection
    const now = Date.now();
    if (now - ws.windowStart > 1000) {
      ws.windowStart = now;
      ws.windowCount = 0;
    }
    if (++ws.windowCount > MAX_MSGS_PER_SEC) return;

    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch (_) {
      return send(ws, { op: 'error', code: 'bad' });
    }
    if (!m || typeof m !== 'object') return;

    switch (m.op) {
      case 'create': {
        leaveRoom(ws);
        const code = newRoomCode();
        if (!code) return send(ws, { op: 'error', code: 'bad' });
        rooms.set(code, { host: ws, guest: null, created: Date.now() });
        ws.room = code;
        ws.role = 'host';
        send(ws, { op: 'created', room: code });
        break;
      }
      case 'join': {
        leaveRoom(ws);
        const code = String(m.room || '').replace(/\D/g, '');
        const room = rooms.get(code);
        if (!room) return send(ws, { op: 'error', code: 'not_found' });
        if (room.guest) return send(ws, { op: 'error', code: 'full' });
        if (room.host === ws) return send(ws, { op: 'error', code: 'bad' });
        pair(code, room.host, ws);
        break;
      }
      case 'quick': {
        leaveRoom(ws);
        const other = queue.find((s) => s !== ws && s.readyState === 1);
        if (other) {
          leaveQueue(other);
          const code = newRoomCode();
          if (!code) return send(ws, { op: 'error', code: 'bad' });
          pair(code, other, ws); // who waited longest hosts
        } else {
          queue.push(ws);
          send(ws, { op: 'queued' });
        }
        break;
      }
      case 'cancel': {
        // only valid while still waiting
        const code = ws.room;
        const room = code && rooms.get(code);
        if (!room || !room.guest) leaveRoom(ws);
        leaveQueue(ws);
        break;
      }
      case 'ping': {
        // app-level keep-alive: also counts as traffic for hosts that put
        // idle servers to sleep
        send(ws, { op: 'pong' });
        break;
      }
      case 'relay': {
        if (ws.peer && ws.peer.readyState === 1 && m.data && typeof m.data === 'object') {
          send(ws.peer, { op: 'relay', data: m.data });
        }
        break;
      }
      default:
        break;
    }
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

// drop dead connections and stale unjoined rooms
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try {
      ws.ping();
    } catch (_) {}
  }
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (!room.guest && now - room.created > ROOM_TTL_MS) {
      rooms.delete(code);
      if (room.host) {
        room.host.room = null;
        send(room.host, { op: 'error', code: 'not_found' });
      }
    }
  }
}, HEARTBEAT_MS);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Tug of War relay listening on port ${PORT} (ws:// and http://…/health)`);
});

// polite shutdown when the host restarts the service
function shutdown() {
  for (const ws of wss.clients) {
    try {
      ws.close(1001, 'server restarting');
    } catch (_) {}
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
