// Pitch Rush online server: usernames, friends, invites and live match relay.
// Run:  npm install ws   then   node server.js
// Env:  PORT (default 8080), ALLOWED_ORIGINS (comma list, optional), DATA_FILE (default ./data.json)
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const fs = require('fs');

const PORT = +process.env.PORT || 8080;
const DATA_FILE = process.env.DATA_FILE || './data.json';
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);

// ---------- storage (in memory, persisted to a JSON file) ----------
let db = { profiles: {}, requests: {}, invites: {}, tokens: {} };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'))); db.invites = {}; } catch (e) {}
let saveT = null;
function save() {
  clearTimeout(saveT);
  saveT = setTimeout(() => {
    const out = JSON.stringify({ profiles: db.profiles, requests: db.requests, tokens: db.tokens });
    fs.writeFile(DATA_FILE, out, () => {});
  }, 1000);
}
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const ID = /^[A-Za-z0-9_-]{1,64}$/, DOCID = /^[A-Za-z0-9_-]{1,140}$/, NAME = /^[a-z0-9_]{3,16}$/;
const ROOM = /^(__lobby|r:[a-z0-9][a-z0-9_.-]{0,47})$/;

// ---------- connections ----------
const conns = new Set();
const rooms = new Map(); // name -> Map(conn -> presence)
const subs = { profiles: new Set(), requests: new Set(), invites: new Set() };
const send = (c, m) => { if (c.ws.readyState === 1) c.ws.send(JSON.stringify(m)); };

// who may see a document
const visible = (col, d, uid) => col === 'profiles' || (d && (d.from === uid || d.to === uid));

function broadcastDoc(col, id, d, prev) {
  for (const c of subs[col]) {
    const can = visible(col, d, c.uid), was = visible(col, prev, c.uid);
    if (can) send(c, { t: 'doc', c: col, id, d });
    else if (was) send(c, { t: 'doc', c: col, id, d: null });
  }
}

// ---------- writes with validation ----------
function write(c, op, col, id, d) {
  if (!db[col] || !DOCID.test(id)) throw 'bad_path';
  const cur = db[col][id], uid = c.uid;
  if (op === 'del') {
    if (col === 'profiles') { if (id !== uid) throw 'forbidden'; }
    else if (!cur || (cur.from !== uid && cur.to !== uid)) throw 'forbidden';
    delete db[col][id]; broadcastDoc(col, id, null, cur); save(); return;
  }
  if (!d || typeof d !== 'object' || JSON.stringify(d).length > 1024) throw 'bad_data';
  let nd;
  if (col === 'profiles') {
    if (id !== uid) throw 'forbidden';
    if (typeof d.name !== 'string' || !NAME.test(d.name)) throw 'bad_name';
    for (const k in db.profiles) if (k !== uid && db.profiles[k].lname === d.name) throw 'name_taken';
    nd = { name: d.name, lname: d.name };
  } else if (col === 'requests') {
    const [from, to] = id.split('__');
    if (op === 'set') {
      if (from !== uid || !to || to === uid || !ID.test(to) || !db.profiles[to]) throw 'forbidden';
      if (cur) throw 'exists';
      nd = { from, to, status: 'pending', ts: Date.now() };
    } else {
      if (!cur || cur.to !== uid || d.status !== 'accepted') throw 'forbidden';
      nd = Object.assign({}, cur, { status: 'accepted' });
    }
  } else { // invites
    const [from, to] = id.split('__');
    if (op === 'set') {
      if (from !== uid || !to || !ID.test(to) || typeof d.room !== 'string' || !/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(d.room)) throw 'forbidden';
      nd = { from, to, room: d.room, ts: Date.now(), status: 'pending' };
    } else {
      if (!cur || cur.to !== uid || d.status !== 'accepted') throw 'forbidden';
      nd = Object.assign({}, cur, { status: 'accepted' });
    }
  }
  db[col][id] = nd; broadcastDoc(col, id, nd, cur); save();
}

// ---------- rooms ----------
function peersOf(room, except) {
  const out = [];
  for (const [c, p] of rooms.get(room) || []) if (c !== except) out.push({ peer: c.peer, by: c.uid, presence: p });
  return out;
}
function leaveRoom(c, name) {
  const r = rooms.get(name); if (!r || !r.has(c)) return;
  r.delete(c); c.rooms.delete(name);
  for (const o of r.keys()) send(o, { t: 'rl', room: name, peer: c.peer });
  if (!r.size) rooms.delete(name);
}
function mergePresence(cur, patch) {
  const n = Object.assign({}, cur);
  for (const k of Object.keys(patch)) { if (patch[k] === null) delete n[k]; else n[k] = patch[k]; }
  return n;
}

// ---------- message handling ----------
function handle(c, m) {
  const ack = (ok, err) => m.r != null && send(c, { t: 'ack', r: m.r, ok, err });
  if (m.t === 'hello') {
    if (!ID.test(String(m.id)) || typeof m.tok !== 'string' || m.tok.length < 16 || m.tok.length > 100) return c.ws.close(4001);
    const h = sha(m.tok);
    if (db.tokens[m.id] && db.tokens[m.id] !== h) { send(c, { t: 'err', code: 'auth' }); return c.ws.close(4003); }
    if (!db.tokens[m.id]) { db.tokens[m.id] = h; save(); }
    c.uid = m.id; return send(c, { t: 'welcome', peer: c.peer });
  }
  if (!c.uid) return c.ws.close(4001);
  try {
    if (m.t === 'sub' && subs[m.c]) {
      subs[m.c].add(c);
      const docs = Object.entries(db[m.c]).filter(([, d]) => visible(m.c, d, c.uid));
      return send(c, { t: 'snap', c: m.c, docs });
    }
    if (m.t === 'set' || m.t === 'upd' || m.t === 'del') { write(c, m.t, m.c, String(m.id), m.d); return ack(true); }
    if (m.t === 'join') {
      if (!ROOM.test(m.room)) throw 'bad_room';
      if (!rooms.has(m.room)) rooms.set(m.room, new Map());
      const r = rooms.get(m.room);
      if (!r.has(c)) {
        if (m.room.startsWith('r:') && r.size >= 2) throw 'room_full';
        r.set(c, {}); c.rooms.add(m.room);
      }
      send(c, { t: 'rj', room: m.room, peers: peersOf(m.room, c) });
      return ack(true);
    }
    if (m.t === 'pres') {
      const r = rooms.get(m.room);
      if (!r || !r.has(c) || !m.p || typeof m.p !== 'object') return;
      const merged = mergePresence(r.get(c), m.p);
      if (JSON.stringify(merged).length > 4096) return;
      merged.uid = c.uid; // the server vouches for identity
      r.set(c, merged);
      for (const o of r.keys()) if (o !== c) send(o, { t: 'rp', room: m.room, peer: c.peer, by: c.uid, presence: merged });
      return;
    }
    if (m.t === 'leave') return leaveRoom(c, m.room);
  } catch (e) { ack(false, typeof e === 'string' ? e : 'error'); }
}

// ---------- server ----------
const wss = new WebSocketServer({
  port: PORT, maxPayload: 8192,
  verifyClient: (info) => !ORIGINS.length || ORIGINS.includes(info.origin),
});
let nextPeer = 1;
wss.on('connection', ws => {
  const c = { ws, uid: null, peer: 'p' + (nextPeer++).toString(36) + crypto.randomBytes(3).toString('hex'), rooms: new Set(), n: 0, alive: true };
  conns.add(c);
  const tick = setInterval(() => { c.n = 0; }, 1000);
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', raw => {
    if (++c.n > 150) return ws.close(4008); // rate limit
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (m && typeof m === 'object') handle(c, m);
  });
  ws.on('close', () => {
    clearInterval(tick); conns.delete(c);
    for (const s of Object.values(subs)) s.delete(c);
    for (const n of [...c.rooms]) leaveRoom(c, n);
  });
  ws.on('error', () => {});
});
setInterval(() => {
  for (const c of conns) { if (!c.alive) { c.ws.terminate(); continue; } c.alive = false; c.ws.ping(); }
}, 20000);
// drop stale invites
setInterval(() => {
  for (const [id, i] of Object.entries(db.invites)) if (Date.now() - i.ts > 600000) write({ uid: i.from }, 'del', 'invites', id);
}, 60000);
console.log('Pitch Rush server listening on port ' + PORT);
