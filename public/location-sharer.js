// Reads the browser position and sends it to the server on a fixed interval.
//
// Why an interval instead of sending every GPS callback? watchPosition can fire many times per second
// with tiny jitter. We keep only the LATEST fix and send it every N seconds - predictable load
// (users x 1/N events per second) no matter how chatty the device GPS is.
// If the user hasn't moved, we still send a heartbeat every HEARTBEAT_MS so they don't go stale.

const MIN_MOVE_METERS = 2;
const HEARTBEAT_MS = 10_000;
const ACK_TIMEOUT_MS = 5_000;

function metersBetween(a, b) {
  const R = 6_371_000;
  const rad = (d) => (d * Math.PI) / 180;
  const h = Math.sin(rad(b.lat - a.lat) / 2) ** 2 +
    Math.cos(rad(a.lat)) * Math.cos(rad(b.lat)) * Math.sin(rad(b.lng - a.lng) / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

export class LocationSharer {
  /**
   * @param {object} opts
   * @param {import('socket.io-client').Socket} opts.socket
   * @param {(fix:{lat:number,lng:number}) => void} opts.onFirstFix
   * @param {(kind:string, detail:object) => void} opts.onEvent  'sent' | 'acked' | 'duplicate' | 'rejected' | 'error' | 'permission'
   */
  constructor({ socket, onFirstFix, onEvent }) {
    this.socket = socket;
    this.onFirstFix = onFirstFix;
    this.onEvent = onEvent;
    this.intervalMs = 3000;
    this.simulate = false;
    this.active = false;
    this.latest = null; // latest fix from GPS or simulator
    this.lastSent = null;
    this.watchId = null;
    this.timer = null;
  }

  async permissionState() {
    try {
      const status = await navigator.permissions.query({ name: 'geolocation' });
      return status.state; // 'granted' | 'prompt' | 'denied'
    } catch {
      return 'unknown';
    }
  }

  start() {
    if (this.active) return;
    this.active = true;
    if (!this.simulate) this.startGps();
    this.restartTimer();
  }

  stop() {
    this.active = false;
    this.stopGps();
    clearInterval(this.timer);
    this.timer = null;
    this.lastSent = null;
    if (this.socket.connected) this.socket.emit('location:stop');
  }

  setIntervalMs(ms) {
    this.intervalMs = ms;
    if (this.active) this.restartTimer();
  }

  setSimulate(enabled) {
    this.simulate = enabled;
    if (!this.active) return;
    if (enabled) this.stopGps();
    else this.startGps();
  }

  startGps() {
    if (!('geolocation' in navigator)) {
      this.onEvent('error', { message: 'This browser has no Geolocation API' });
      return;
    }
    // Calling watchPosition is what triggers the browser's permission prompt.
    this.watchId = navigator.geolocation.watchPosition(
      (pos) => {
        const first = !this.latest;
        this.latest = {
          lat: pos.coords.latitude,
          lng: pos.coords.longitude,
          accuracy: pos.coords.accuracy,
          heading: Number.isFinite(pos.coords.heading) ? pos.coords.heading : null,
          speed: Number.isFinite(pos.coords.speed) ? pos.coords.speed : null,
          source: 'gps',
        };
        this.onEvent('permission', { state: 'granted' });
        if (first) {
          this.onFirstFix(this.latest);
          this.tick(); // send the first fix immediately instead of waiting a full interval
        }
      },
      (err) => {
        const state = err.code === err.PERMISSION_DENIED ? 'denied' : 'error';
        this.onEvent('permission', { state, message: err.message });
      },
      { enableHighAccuracy: true, maximumAge: 5_000, timeout: 20_000 },
    );
  }

  stopGps() {
    if (this.watchId !== null) navigator.geolocation.clearWatch(this.watchId);
    this.watchId = null;
  }

  restartTimer() {
    clearInterval(this.timer);
    this.timer = setInterval(() => this.tick(), this.intervalMs);
  }

  /** Random walk ~1.4 m/s from the current position (or a fallback start point). */
  simulateStep(fallbackCenter) {
    // Without a real fix, start ~0-300 m from the map centre so two simulated users don't overlap.
    const base = this.latest ?? {
      lat: fallbackCenter.lat + (Math.random() - 0.5) * 0.005,
      lng: fallbackCenter.lng + (Math.random() - 0.5) * 0.005,
      heading: Math.random() * 360,
    };
    const heading = ((base.heading ?? 0) + (Math.random() - 0.5) * 50 + 360) % 360;
    const meters = 1.4 * (this.intervalMs / 1000);
    const rad = (heading * Math.PI) / 180;
    const lat = base.lat + (meters * Math.cos(rad)) / 111_320;
    const lng = base.lng + (meters * Math.sin(rad)) / (111_320 * Math.cos((base.lat * Math.PI) / 180));
    const first = !this.latest;
    this.latest = { lat, lng, accuracy: 5, heading, speed: 1.4, source: 'simulated' };
    if (first) this.onFirstFix(this.latest);
  }

  tick() {
    if (!this.active) return;
    if (this.simulate) this.simulateStep(this.fallbackCenter?.() ?? { lat: 19.076, lng: 72.8777 });
    if (!this.latest || !this.socket.connected) return;

    const moved = !this.lastSent || metersBetween(this.lastSent, this.latest) >= MIN_MOVE_METERS;
    const heartbeatDue = !this.lastSent || Date.now() - this.lastSent.at >= HEARTBEAT_MS;
    if (!moved && !heartbeatDue) return;

    const payload = { eventId: crypto.randomUUID(), ...this.latest, clientTs: Date.now() };
    this.lastSent = { lat: payload.lat, lng: payload.lng, at: Date.now() };
    this.send(payload, 1);
  }

  /** Emit with an ack. If no ack arrives in time, retry ONCE with the SAME eventId (server dedupes). */
  send(payload, attempt) {
    this.onEvent('sent', { payload, attempt });
    this.socket.timeout(ACK_TIMEOUT_MS).emit('location:update', payload, (timeoutErr, res) => {
      if (timeoutErr) {
        if (attempt < 2 && this.socket.connected) return this.send(payload, attempt + 1);
        return this.onEvent('error', { message: 'no ack from server', eventId: payload.eventId });
      }
      if (res.ok && res.duplicate) this.onEvent('duplicate', res);
      else if (res.ok) this.onEvent('acked', res);
      else this.onEvent('rejected', res);
    });
  }
}
