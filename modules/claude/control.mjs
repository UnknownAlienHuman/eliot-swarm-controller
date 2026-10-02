// One persistent, multiplexed local connection to the host. A lost reply is
// never a native retry. Same wire protocol as the Muse module link:
// newline-delimited JSON-RPC 2.0 over the host endpoint (pipe/socket).
import net from 'node:net';
import { randomUUID } from 'node:crypto';

export class Control {
  constructor(endpoint, credential, limit = 1048576) {
    this.endpoint = endpoint;
    this.credential = credential;
    this.limit = limit;
    this.pending = new Map();
    this.socket = null;
  }
  async connect() {
    const socket = net.createConnection(this.endpoint);
    this.socket = socket;
    socket.setEncoding('utf8');
    let buffer = '';
    socket.on('data', chunk => {
      buffer += chunk;
      let end;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
        if (Buffer.byteLength(line) > this.limit) { socket.destroy(new Error('FRAME_TOO_LARGE')); return; }
        try {
          const packet = JSON.parse(line);
          if (packet.jsonrpc !== '2.0' || typeof packet.id !== 'string') throw new Error('INVALID_FRAME');
          const entry = this.pending.get(packet.id);
          if (!entry) continue;
          this.pending.delete(packet.id); clearTimeout(entry.timer);
          if (packet.error) {
            const error = new Error(packet.error.message);
            error.code = packet.error.data?.code ?? 'RPC_ERROR'; entry.reject(error);
          } else if (Object.hasOwn(packet, 'result')) entry.resolve(packet.result);
          else entry.reject(new Error('MISSING_RESULT'));
        } catch { socket.destroy(new Error('PROTOCOL_ERROR')); return; }
      }
      if (Buffer.byteLength(buffer) > this.limit) socket.destroy(new Error('FRAME_TOO_LARGE'));
    });
    socket.on('error', () => {});
    socket.on('close', () => {
      if (this.socket === socket) this.socket = null;
      for (const entry of this.pending.values()) {
        clearTimeout(entry.timer); entry.reject(new Error('CONTROL_DISCONNECTED'));
      }
      this.pending.clear();
    });
    await new Promise((resolve, reject) => { socket.once('connect', resolve); socket.once('error', reject); });
    return await this.call('client.hello', this.credential);
  }
  call(method, params) {
    const socket = this.socket;
    if (!socket || socket.destroyed) return Promise.reject(new Error('CONTROL_DISCONNECTED'));
    const id = randomUUID();
    const line = JSON.stringify({ jsonrpc: '2.0', id, method, params });
    if (Buffer.byteLength(line) > this.limit) return Promise.reject(new Error('FRAME_TOO_LARGE'));
    // At most a handful of commands/snapshot writes, never native token deltas.
    if (socket.writableLength > this.limit) return Promise.reject(new Error('CONTROL_BACKPRESSURE'));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => socket.destroy(new Error('CONTROL_TIMEOUT')), 30000);
      this.pending.set(id, { resolve, reject, timer });
      socket.write(line + '\n', error => { if (error) socket.destroy(error); });
    });
  }
  close() { this.socket?.destroy(); }
}
