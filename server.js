const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_, res) => res.send('ok'));
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 6e6 });

const rooms = {};
const uid = () => Math.random().toString(36).slice(2, 10);
const clamp = (n, a, b, d) => (Number.isFinite(+n) ? Math.min(b, Math.max(a, Math.round(+n))) : d);
const shuffle = a => { for (let i = a.length - 1; i > 0; i--) { const j = Math.random() * (i + 1) | 0; [a[i], a[j]] = [a[j], a[i]]; } return a; };
function newCode() {
  let c;
  do { c = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPQRSTUVWXYZ'[Math.random() * 24 | 0]).join(''); } while (rooms[c]);
  return c;
}
const mkPlayer = (name, sid) => ({ id: uid(), name: name.slice(0, 20), sid, online: true, ready: false, img: null, desc: '', sum: 0, n: 0 });
const online = r => Object.values(r.players).filter(p => p.online);
const owner = r => r.players[r.queue[r.idx]];

function scores(r) {
  return Object.values(r.players)
    .map(p => ({ id: p.id, name: p.name, n: p.n, score: p.n ? Math.round(p.sum / p.n * 100) / 10 : 0 }))
    .sort((a, b) => b.score - a.score);
}

function view(r, p) {
  const o = r.phase === 'rate' ? owner(r) : null;
  return {
    code: r.code, phase: r.phase, me: p.id, hostId: r.hostId, hostGone: r.hostGone,
    settings: r.settings, round: r.round, idx: r.idx, total: r.queue.length,
    mine: { img: p.img, desc: p.desc, ready: p.ready },
    myRating: r.ratings[p.id] ?? null,
    players: Object.values(r.players).map(q => ({
      id: q.id, name: q.name, online: q.online,
      ready: r.phase === 'rate' ? (q.id === (o && o.id) || r.ratings[q.id] !== undefined) : q.ready,
    })),
    final: r.phase === 'final' ? scores(r) : null,
  };
}
function push(r) { online(r).forEach(p => io.to(p.sid).emit('state', view(r, p))); }
function itemFor(r) {
  const o = owner(r);
  return { idx: r.idx, total: r.queue.length, img: o.img, desc: o.desc, ownerId: o.id, ownerName: o.name, ms: Math.max(0, r.endsAt - Date.now()) };
}

function startSubmit(r) {
  clearTimeout(r.timer);
  r.phase = 'submit'; r.round++; r.queue = []; r.idx = -1; r.ratings = {};
  Object.values(r.players).forEach(p => { p.ready = false; p.img = null; p.desc = ''; });
  push(r);
}
function checkSubmit(r) {
  if (r.phase !== 'submit') return;
  const on = online(r);
  if (on.length && on.every(p => p.ready)) startRate(r);
}
function startRate(r) {
  r.phase = 'rate';
  r.queue = shuffle(Object.values(r.players).filter(p => p.img).map(p => p.id));
  r.idx = -1;
  nextItem(r);
}
function commit(r) {
  if (r.idx < 0 || !r.players[r.queue[r.idx]]) return;
  const o = owner(r);
  Object.values(r.ratings).forEach(v => { o.sum += v; o.n++; });
}
function nextItem(r) {
  clearTimeout(r.timer);
  commit(r);
  r.ratings = {};
  r.idx++;
  if (r.idx >= r.queue.length) {
    if (r.round < r.settings.rounds) return startSubmit(r);
    r.phase = 'final'; return push(r);
  }
  const ms = r.settings.rateSecs * 1000;
  r.endsAt = Date.now() + ms;
  const tick = r.idx;
  r.timer = setTimeout(() => { if (r.phase === 'rate' && r.idx === tick) nextItem(r); }, ms + 300);
  online(r).forEach(p => io.to(p.sid).emit('item', itemFor(r)));
  push(r);
  checkRate(r);
}
function checkRate(r) {
  if (r.phase !== 'rate' || r.idx < 0) return;
  const o = owner(r);
  const raters = online(r).filter(p => p.id !== o.id);
  if (raters.every(p => r.ratings[p.id] !== undefined)) nextItem(r);
}
function closeRoom(r, msg) {
  clearTimeout(r.timer);
  online(r).forEach(p => io.to(p.sid).emit('closed', msg));
  delete rooms[r.code];
}
function dropPlayer(r, p, final) {
  p.online = false;
  if (final) p.left = true;
  if (r.phase === 'lobby' && final) {
    delete r.players[p.id];
    if (p.id === r.hostId) return closeRoom(r, 'The host left, so the room closed.');
  }
  if (p.id === r.hostId && final) r.hostGone = true;
  if (!online(r).length) {
    clearTimeout(r.timer);
    r.cleanup = setTimeout(() => { if (rooms[r.code] && !online(rooms[r.code]).length) delete rooms[r.code]; }, 10 * 60 * 1000);
    return;
  }
  push(r);
  checkSubmit(r); checkRate(r);
}

io.on('connection', socket => {
  const ctx = () => {
    const r = rooms[socket.data.code];
    const p = r && r.players[socket.data.pid];
    return r && p && p.sid === socket.id ? { r, p } : {};
  };
  const attach = (r, p, cb) => {
    p.sid = socket.id; p.online = true;
    socket.data = { code: r.code, pid: p.id };
    clearTimeout(r.cleanup);
    cb && cb({ ok: true, pid: p.id, code: r.code });
    push(r);
    if (r.phase === 'rate' && r.idx >= 0) socket.emit('item', itemFor(r));
  };

  socket.on('host', ({ name }, cb) => {
    name = String(name || '').trim();
    if (!name) return cb({ err: 'Enter a display name first.' });
    const r = { code: newCode(), hostId: null, hostGone: false, phase: 'lobby', settings: { rounds: 3, rateSecs: 20 },
      round: 0, players: {}, queue: [], idx: -1, ratings: {}, timer: null, endsAt: 0 };
    const p = mkPlayer(name, socket.id);
    r.hostId = p.id; r.players[p.id] = p; rooms[r.code] = r;
    attach(r, p, cb);
  });
  socket.on('join', ({ name, code }, cb) => {
    name = String(name || '').trim(); code = String(code || '').trim().toUpperCase();
    if (!name) return cb({ err: 'Enter a display name first.' });
    const r = rooms[code];
    if (!r) return cb({ err: 'No room with that code.' });
    if (r.phase !== 'lobby') return cb({ err: 'That game has already started.' });
    if (Object.keys(r.players).length >= 12) return cb({ err: 'That room is full (12 players).' });
    if (Object.values(r.players).some(q => q.name.toLowerCase() === name.toLowerCase())) return cb({ err: 'Someone in that room already has that name.' });
    const p = mkPlayer(name, socket.id);
    r.players[p.id] = p;
    attach(r, p, cb);
  });
  socket.on('rejoin', ({ pid, code }, cb) => {
    const r = rooms[code], p = r && r.players[pid];
    if (!p || p.left) return cb({ err: 'gone' });
    attach(r, p, cb);
  });
  socket.on('settings', s => {
    const { r, p } = ctx();
    if (!r || r.phase !== 'lobby' || p.id !== r.hostId) return;
    r.settings = { rounds: clamp(s.rounds, 1, 10, 3), rateSecs: clamp(s.rateSecs, 5, 120, 20) };
    push(r);
  });
  socket.on('start', () => {
    const { r, p } = ctx();
    if (!r || r.phase !== 'lobby' || p.id !== r.hostId) return;
    Object.values(r.players).forEach(q => { if (!q.online) delete r.players[q.id]; q.sum = 0; q.n = 0; });
    if (Object.keys(r.players).length < 2) return socket.emit('toast', 'You need at least 2 players.');
    r.round = 0; startSubmit(r);
  });
  socket.on('ready', ({ img, desc }) => {
    const { r, p } = ctx();
    if (!r || r.phase !== 'submit') return;
    if (typeof img === 'string' && img.startsWith('data:image/') && img.length < 3e6) p.img = img;
    if (!p.img) return socket.emit('toast', 'Upload a picture first.');
    p.desc = String(desc || '').slice(0, 200);
    p.ready = true;
    push(r); checkSubmit(r);
  });
  socket.on('unready', () => {
    const { r, p } = ctx();
    if (!r || r.phase !== 'submit') return;
    p.ready = false; push(r);
  });
  socket.on('rate', v => {
    const { r, p } = ctx();
    if (!r || r.phase !== 'rate' || r.idx < 0 || owner(r).id === p.id) return;
    v = clamp(v, 1, 10, 0);
    if (!v || r.ratings[p.id] !== undefined) return;
    r.ratings[p.id] = v;
    push(r); checkRate(r);
  });
  socket.on('again', () => {
    const { r, p } = ctx();
    if (!r || r.phase !== 'final' || p.id !== r.hostId || r.hostGone) return;
    Object.values(r.players).forEach(q => { if (!q.online) delete r.players[q.id]; q.sum = 0; q.n = 0; q.ready = false; q.img = null; });
    r.phase = 'lobby'; r.round = 0; r.queue = []; r.idx = -1; r.ratings = {};
    push(r);
  });
  socket.on('leave', () => { const { r, p } = ctx(); if (r) { dropPlayer(r, p, true); socket.data = {}; } });
  socket.on('disconnect', () => { const { r, p } = ctx(); if (r) dropPlayer(r, p, false); });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Hear Me Out running on port ' + PORT));
