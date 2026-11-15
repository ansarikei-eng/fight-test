const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const cors = require('cors');
const path = require('path');
const crypto = require('crypto');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/api/room/ws' });

const rooms = new Map();
const ROOM_LIFETIME_MS = 10 * 60 * 1000;
const ice = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ],
  relay: false
};

function makeRoom(offer = null) {
  let code;
  do {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    code = 'S';
    for (let i = 0; i < 3; i++) {
      code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
  } while (rooms.has(code));

  const room = {
    code,
    secret: crypto.randomBytes(16).toString('hex'),
    offer,
    answer: null,
    signals: new Map(),
    waiters: new Set(),
    hostSocket: null
  };
  rooms.set(code, room);

  const expiry = setTimeout(() => {
    if (rooms.get(code) === room) {
      rooms.delete(code);
      if (room.hostSocket && room.hostSocket.readyState === room.hostSocket.OPEN) {
        room.hostSocket.close(4404, 'Room expired');
      }
    }
  }, ROOM_LIFETIME_MS);
  expiry.unref();
  return room;
}

function notifyRoom(room) {
  for (const waiter of room.waiters) waiter();
}

function waitForRoom(room, predicate, timeoutMs) {
  if (predicate()) return Promise.resolve(true);

  return new Promise((resolve) => {
    let timer;
    const finish = (matched) => {
      clearTimeout(timer);
      room.waiters.delete(check);
      resolve(matched);
    };
    const check = () => {
      if (predicate()) finish(true);
    };
    timer = setTimeout(() => finish(false), timeoutMs);
    room.waiters.add(check);
  });
}

function findRoom(code, res) {
  const room = rooms.get(String(code || '').toUpperCase());
  if (!room) {
    res.status(404).json({ error: 'Room not found' });
    return null;
  }
  return room;
}

app.get('/api/ice', (req, res) => {
  res.json(ice);
});

app.get('/api/room/join', (req, res) => {
  const room = findRoom(req.query.code, res);
  if (!room) return;
  if (!room.offer) return res.status(404).json({ error: 'Room offer is not ready' });
  res.json({ offer: room.offer, ice });
});

app.post('/api/room/answer', (req, res) => {
  const room = findRoom(req.query.code, res);
  if (!room) return;
  if (!req.body || !req.body.answer) return res.status(400).json({ error: 'Answer is required' });
  room.answer = req.body.answer;
  notifyRoom(room);
  if (room.hostSocket && room.hostSocket.readyState === room.hostSocket.OPEN) {
    room.hostSocket.send(JSON.stringify({ t: 'answer', desc: room.answer }));
  }
  res.json({ success: true });
});

app.post('/api/room/close', (req, res) => {
  const room = findRoom(req.query.code, res);
  if (!room) return;
  if (req.query.secret !== room.secret) return res.status(403).json({ error: 'Invalid room secret' });
  rooms.delete(room.code);
  notifyRoom(room);
  res.json({ success: true });
});

app.all('/api/room', async (req, res, next) => {
  try {
    const { action, code, secret } = req.query;

    if (action === 'signal') {
      const signalRoom = [...rooms.values()].find((candidate) => candidate.secret === req.query.sid);
      if (!signalRoom) {
        return res.status(404).json({ error: 'Room not found' });
      }
      const signalKey = `${req.query.kind}:${req.query.n}`;

      if (req.method === 'POST') {
        if (!req.body || !req.body.desc) return res.status(400).json({ error: 'Signal description is required' });
        signalRoom.signals.set(signalKey, req.body.desc);
        notifyRoom(signalRoom);
        return res.json({ success: true });
      }
      if (req.method === 'GET') {
        const waitMs = Math.min(Math.max(Number(req.query.wait) || 0, 0), 25) * 1000;
        if (!signalRoom.signals.has(signalKey) && waitMs) {
          await waitForRoom(signalRoom, () => signalRoom.signals.has(signalKey), waitMs);
        }
        const desc = signalRoom.signals.get(signalKey) || null;
        signalRoom.signals.delete(signalKey);
        return res.json({ desc });
      }
      return res.status(405).json({ error: 'Method not allowed' });
    }

    if (req.method === 'POST' && !code) {
      if (!req.body || !req.body.offer) return res.status(400).json({ error: 'Offer is required' });
      const room = makeRoom(req.body.offer);
      return res.json({ code: room.code, secret: room.secret });
    }

    if (!code) return res.status(400).json({ error: 'Room code is required' });
    const room = findRoom(code, res);
    if (!room) return;

    if (req.method === 'DELETE') {
      if (secret !== room.secret) return res.status(403).json({ error: 'Invalid room secret' });
      rooms.delete(room.code);
      notifyRoom(room);
      return res.json({ success: true });
    }
    if (req.method === 'POST' && action === 'answer') {
      if (!req.body || !req.body.answer) return res.status(400).json({ error: 'Answer is required' });
      room.answer = req.body.answer;
      notifyRoom(room);
      return res.json({ success: true });
    }
    if (req.method === 'GET' && secret) {
      if (secret !== room.secret) return res.status(403).json({ error: 'Invalid room secret' });
      const waitMs = Math.min(Math.max(Number(req.query.wait) || 0, 0), 25) * 1000;
      if (!room.answer && waitMs) await waitForRoom(room, () => Boolean(room.answer), waitMs);
      return res.json({ answer: room.answer });
    }
    if (req.method === 'GET' && room.offer) return res.json({ offer: room.offer, ice });
    return res.status(405).json({ error: 'Method not allowed' });
  } catch (error) {
    next(error);
  }
});

app.use(express.static(path.join(__dirname, '../')));

wss.on('connection', (ws, request) => {
  const params = new URL(request.url, 'http://localhost').searchParams;
  let room;

  if (params.has('code')) {
    room = rooms.get(String(params.get('code')).toUpperCase());
    if (!room) return ws.close(4404, 'Room expired');
    if (params.get('secret') !== room.secret) return ws.close(4403, 'Invalid room secret');
    room.hostSocket = ws;
    ws.send(JSON.stringify({ t: 'resumed', ready: Boolean(room.offer) }));
    if (room.answer) ws.send(JSON.stringify({ t: 'answer', desc: room.answer }));
  } else {
    room = makeRoom();
    room.hostSocket = ws;
    ws.send(JSON.stringify({ t: 'room', code: room.code, secret: room.secret, ice }));
  }

  ws.on('message', (message) => {
    try {
      const msgStr = message.toString();
      const data = JSON.parse(msgStr);
      if (data.t === 'offer' && data.desc) {
        room.offer = data.desc;
        if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ t: 'ready' }));
        notifyRoom(room);
      } else if (data.t === 'close') {
        ws.close(1000, 'Room client closed');
      }
    } catch (error) {
      console.error('Invalid WebSocket message:', error);
    }
  });

  ws.on('close', () => {
    if (room && room.hostSocket === ws) room.hostSocket = null;
  });
});

app.use((error, req, res, next) => {
  console.error('Request error:', error);
  if (!res.headersSent) res.status(500).json({ error: 'Internal server error' });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Sansad Multiplayer Server running at http://localhost:${PORT}`);
});