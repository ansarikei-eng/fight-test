import { r as PeerTransport } from './rooms-CujU6IZK.js';

function roomSocketUrl(query) {
  const url = new URL(`/api/room/ws${query}`, window.location.origin);
  url.protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.toString();
}

function readMessage(event) {
  if (typeof event.data !== 'string') return null;
  try {
    return JSON.parse(event.data);
  } catch {
    return null;
  }
}

class RelayPeer {
  constructor(socket, role, handlers) {
    this.socket = socket;
    this.role = role;
    this.handlers = handlers;
    this.closed = false;
    this.isConnected = false;
    this.samples = [];
    this.pendingPings = new Map();
    this.pingSequence = 0;
    this.quietOk = false;
    this.connectResolve = null;
    this.connectReject = null;
    this.connected = new Promise((resolve, reject) => {
      this.connectResolve = resolve;
      this.connectReject = reject;
    });

    socket.binaryType = 'arraybuffer';
    socket.onmessage = (event) => this.receive(event);
    socket.onclose = () => this.handleClose();
    socket.onerror = () => {};
  }

  receive(event) {
    if (event.data instanceof ArrayBuffer) {
      this.handlers.onInput?.(event.data);
      return;
    }

    const message = readMessage(event);
    if (!message) return;

    if (message.t === 'joined' || message.t === 'peer-connected') {
      this.markConnected();
    } else if (message.t === 'relay' && message.channel === 'ctrl') {
      this.handlers.onCtrl?.(message.data);
    } else if (message.t === 'relay' && message.channel === 'input-text') {
      this.handlers.onInput?.(message.data);
    } else if (message.t === 'heartbeat-ack') {
      const sentAt = this.pendingPings.get(message.id);
      if (sentAt !== undefined) {
        this.pendingPings.delete(message.id);
        this.samples.push(performance.now() - sentAt);
        if (this.samples.length > 40) this.samples.shift();
      }
    } else if (message.t === 'peer-disconnected') {
      this.handlers.onLost?.();
    } else if (message.t === 'room-closed') {
      this.handlers.onState?.('closed');
    }
  }

  markConnected() {
    if (this.isConnected || this.closed) return;
    this.isConnected = true;
    this.handlers.onState?.('connected');
    this.connectResolve();
    this.pingTimer = window.setInterval(() => this.ping(), 1000);
    this.ping();
  }

  ping() {
    if (this.socket.readyState !== WebSocket.OPEN) return;
    const id = ++this.pingSequence;
    this.pendingPings.set(id, performance.now());
    if (this.pendingPings.size > 40) this.pendingPings.delete(this.pendingPings.keys().next().value);
    this.socket.send(JSON.stringify({ t: 'heartbeat', id }));
  }

  handleClose() {
    if (this.closed) return;
    this.closed = true;
    window.clearInterval(this.pingTimer);
    if (!this.isConnected) this.connectReject(new Error('The room connection closed before the player joined.'));
    this.handlers.onState?.('closed');
  }

  send(message) {
    if (this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(JSON.stringify({ t: 'relay', channel: 'ctrl', data: message }));
    }
  }

  sendInput(data) {
    if (this.socket.readyState !== WebSocket.OPEN || this.socket.bufferedAmount >= 128 * 1024) return;
    if (typeof data === 'string') {
      this.socket.send(JSON.stringify({ t: 'relay', channel: 'input-text', data }));
    } else {
      this.socket.send(data);
    }
  }

  latency() {
    if (!this.samples.length) return null;
    const sorted = [...this.samples].sort((a, b) => a - b);
    let jitter = 0;
    for (let i = 1; i < this.samples.length; i++) {
      jitter += Math.abs(this.samples[i] - this.samples[i - 1]);
    }
    return {
      last: this.samples[this.samples.length - 1],
      avg: this.samples.reduce((sum, sample) => sum + sample, 0) / this.samples.length,
      median: sorted[Math.floor(sorted.length / 2)],
      min: sorted[0],
      jitter: this.samples.length > 1 ? jitter / (this.samples.length - 1) : 0
    };
  }

  async linkInfo() {
    return {
      kind: 'relay',
      local: 'server',
      remote: 'server',
      protocol: window.location.protocol === 'https:' ? 'wss' : 'ws',
      iceRtt: this.latency()?.median ?? null,
      relayProtocol: 'websocket'
    };
  }

  forceRestart() {
    this.ping();
    return Promise.resolve();
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    window.clearInterval(this.pingTimer);
    if (!this.isConnected) this.connectReject(new Error('Cancelled'));
    if (this.socket.readyState === WebSocket.OPEN || this.socket.readyState === WebSocket.CONNECTING) {
      this.socket.close(1000, 'Room client closed');
    }
  }
}

function relayHost(handlers, signal) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(roomSocketUrl('?transport=relay'));
    let peer = null;
    let settled = false;
    const timeout = window.setTimeout(() => fail(new Error('Could not open the room connection.')), 12000);
    const abortHandshake = () => {
      if (peer) peer.close();
      else fail(new Error('Cancelled'));
    };

    const cleanup = () => {
      window.clearTimeout(timeout);
      socket.removeEventListener('message', onMessage);
      socket.removeEventListener('error', onError);
      socket.removeEventListener('close', onClose);
      signal?.removeEventListener('abort', abortHandshake);
    };
    const fail = (error) => {
      if (settled) return;
      settled = true;
      cleanup();
      socket.close();
      reject(error);
    };
    const onMessage = (event) => {
      const message = readMessage(event);
      if (message?.t !== 'room') return fail(new Error('The server did not create a room.'));
      settled = true;
      cleanup();
      if (signal?.aborted) {
        socket.close();
        return reject(new Error('Cancelled'));
      }
      peer = new RelayPeer(socket, 'host', handlers);
      signal?.addEventListener('abort', abortHandshake, { once: true });
      peer.socket.addEventListener('close', () => signal?.removeEventListener('abort', abortHandshake), { once: true });
      resolve({ peer, code: message.code, connected: peer.connected });
    };
    const onError = () => fail(new Error('Could not connect to the room server.'));
    const onClose = (event) => fail(new Error(event.reason || 'The room server closed the connection.'));

    socket.addEventListener('message', onMessage);
    socket.addEventListener('error', onError);
    socket.addEventListener('close', onClose);
    if (signal?.aborted) abortHandshake();
    else signal?.addEventListener('abort', abortHandshake, { once: true });
  });
}

function relayJoin(code, handlers) {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(roomSocketUrl(`?join=${encodeURIComponent(code)}`));
    let settled = false;
    const timeout = window.setTimeout(() => fail(new Error('Timed out joining the room.')), 12000);

    const cleanup = () => {
      window.clearTimeout(timeout);
      socket.removeEventListener('message', onMessage);
      socket.removeEventListener('error', onError);
      socket.removeEventListener('close', onClose);
    };
    const fail = (error) => {
      if (settled) return;
      settled = true;
      cleanup();
      socket.close();
      reject(error);
    };
    const onMessage = (event) => {
      const message = readMessage(event);
      if (message?.t !== 'joined') return fail(new Error('The room server rejected the join request.'));
      settled = true;
      cleanup();
      const peer = new RelayPeer(socket, 'guest', handlers);
      peer.markConnected();
      resolve(peer);
    };
    const onError = () => fail(new Error('Could not connect to the room server.'));
    const onClose = (event) => {
      const reason = event.code === 4409 ? 'The room is full.' :
        event.code === 4404 ? 'Room not found or host disconnected.' :
          event.reason || 'The room server closed the connection.';
      fail(new Error(reason));
    };

    socket.addEventListener('message', onMessage);
    socket.addEventListener('error', onError);
    socket.addEventListener('close', onClose);
  });
}

PeerTransport.host = relayHost;
PeerTransport.join = relayJoin;
