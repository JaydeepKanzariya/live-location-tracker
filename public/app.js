// Wires auth check, Socket.IO events, the map and the sidebar together.
/* global io */
import { MapView, colorFor, initials } from './map-view.js';
import { LocationSharer } from './location-sharer.js';

const $ = (id) => document.getElementById(id);
const stats = { sent: 0, acked: 0, dupes: 0, rejected: 0 };
let staleAfterMs = 30_000;

// ---------- 1. Who am I? (session cookie -> /api/me) ----------
const meResponse = await fetch('/api/me', { credentials: 'same-origin' });
if (meResponse.status === 401) {
  showLogin();
} else {
  const { user } = await meResponse.json();
  startApp(user);
}

function showLogin() {
  $('login-view').hidden = false;
  const error = new URLSearchParams(location.search).get('error');
  if (error) {
    const messages = {
      login_failed: 'Login failed. Please try again.',
      login_expired: 'Your login attempt expired. Please try again.',
      session_expired: 'Your session expired. Please sign in again.',
    };
    $('login-error').textContent = messages[error] ?? 'Something went wrong.';
    $('login-error').hidden = false;
  }
}

function startApp(user) {
  $('app-view').hidden = false;
  $('me-name').textContent = user.name;
  $('me-email').textContent = user.email ?? user.id;
  $('me-avatar').textContent = initials(user.name);
  $('me-avatar').style.setProperty('--c', colorFor(user.id));

  const mapView = new MapView('map');
  mapView.setMyUserId(user.id);

  // ---------- 2. Socket.IO: the session cookie authenticates the handshake ----------
  const socket = io({ withCredentials: true, transports: ['websocket', 'polling'] });

  socket.on('connect', () => setConnStatus('ok', 'Live'));
  socket.on('disconnect', (reason) => {
    setConnStatus('warn', 'Reconnecting…');
    log('bad', `socket disconnected (${reason})`);
  });
  socket.on('connect_error', (err) => {
    if (err.message === 'unauthorized') {
      // Session gone (expired / server restarted with memory store) -> back to login.
      socket.disconnect();
      location.href = '/?error=session_expired';
      return;
    }
    setConnStatus('bad', 'Offline');
  });
  socket.on('auth:expired', () => (location.href = '/?error=session_expired'));

  socket.on('session:ready', ({ settings }) => {
    staleAfterMs = settings.staleAfterMs;
    log('info', `authenticated as ${user.name} (sub ${user.id.slice(0, 8)}…)`);
  });

  socket.on('presence:snapshot', (entries) => {
    entries.forEach((e) => mapView.upsert(e));
    renderUserList();
    log('info', `snapshot: ${entries.length} user(s) already on the map`);
  });

  // Every marker move comes from here: the event made the full trip browser -> socket -> Kafka -> consumer -> socket.
  socket.on('location:broadcast', (entry) => {
    mapView.upsert(entry);
    renderUserList();
    if (entry.userId !== user.id) {
      log('info', `${entry.displayName} moved (p${entry.kafka?.partition}@${entry.kafka?.offset})`);
    }
  });

  socket.on('user:offline', ({ userId, reason }) => {
    const name = mapView.entries.get(userId)?.data.displayName ?? userId;
    mapView.remove(userId);
    renderUserList();
    log('bad', `${name} left the map (${reason})`);
  });

  socket.on('location:error', (res) => log('bad', `server rejected update: ${res.error}`));

  // ---------- 3. Sharing my location ----------
  const sharer = new LocationSharer({
    socket,
    onFirstFix: (fix) => mapView.centerOn(fix.lat, fix.lng),
    onEvent: handleSharerEvent,
  });
  sharer.fallbackCenter = () => mapView.map.getCenter();
  sharer.setIntervalMs(Number($('interval-select').value));

  sharer.permissionState().then((state) => setPermissionText(state));

  $('share-btn').addEventListener('click', () => {
    if (sharer.active) {
      sharer.stop();
      $('share-btn').textContent = 'Start sharing';
      $('share-btn').classList.replace('btn--danger', 'btn--primary');
      log('info', 'stopped sharing');
    } else {
      sharer.start();
      $('share-btn').textContent = 'Stop sharing';
      $('share-btn').classList.replace('btn--primary', 'btn--danger');
      log('info', sharer.simulate ? 'started sharing (simulated)' : 'started sharing - waiting for GPS…');
    }
  });
  $('interval-select').addEventListener('change', (e) => sharer.setIntervalMs(Number(e.target.value)));
  $('simulate-toggle').addEventListener('change', (e) => sharer.setSimulate(e.target.checked));

  function handleSharerEvent(kind, detail) {
    switch (kind) {
      case 'sent':
        stats.sent += 1;
        if (detail.attempt > 1) log('bad', `retrying ${detail.payload.eventId.slice(0, 8)} (same eventId)`);
        break;
      case 'acked':
        stats.acked += 1;
        $('stat-kafka').textContent = `partition ${detail.partition}, offset ${detail.offset}`;
        log('ok', `sent → Kafka p${detail.partition}@${detail.offset}`);
        break;
      case 'duplicate':
        stats.dupes += 1;
        log('info', `server ignored duplicate ${detail.eventId.slice(0, 8)}`);
        break;
      case 'rejected':
        stats.rejected += 1;
        log('bad', `rejected: ${detail.error}`);
        break;
      case 'error':
        log('bad', detail.message);
        break;
      case 'permission':
        setPermissionText(detail.state, detail.message);
        if (detail.state === 'denied') log('bad', 'location permission denied - enable it or use "Simulate walking"');
        break;
    }
    renderStats();
  }

  // ---------- 4. Stored history (written by the DB processor) ----------
  $('history-btn').addEventListener('click', async () => {
    const res = await fetch('/api/history?limit=300');
    const body = await res.json();
    if (!body.enabled) return log('bad', 'history API needs DATABASE_URL (processor is in file-simulation mode)');
    const shown = mapView.toggleHistory(body.points);
    $('history-btn').textContent = shown ? 'Hide my stored history' : 'Show my stored history';
    if (shown) log('info', `loaded ${body.points.length} stored history points from Postgres`);
  });

  // ---------- 5. UI helpers ----------
  function renderUserList() {
    const list = $('user-list');
    const users = mapView.list().sort((a, b) => (a.userId === user.id ? -1 : b.userId === user.id ? 1 : a.displayName.localeCompare(b.displayName)));
    $('user-count').textContent = users.length;
    list.replaceChildren();
    if (users.length === 0) {
      const li = document.createElement('li');
      li.className = 'empty';
      li.textContent = 'Nobody is sharing yet.';
      list.append(li);
      return;
    }
    for (const u of users) {
      const li = document.createElement('li');
      const ageSec = Math.round((Date.now() - u.receivedAt) / 1000);
      if (Date.now() - u.receivedAt > staleAfterMs / 2) li.classList.add('is-stale');

      const avatar = document.createElement('div');
      avatar.className = 'avatar';
      avatar.style.setProperty('--c', colorFor(u.userId));
      avatar.textContent = initials(u.displayName);

      const meta = document.createElement('div');
      meta.className = 'meta';
      const name = document.createElement('strong');
      name.textContent = u.userId === user.id ? `${u.displayName} (you)` : u.displayName;
      const sub = document.createElement('small');
      sub.textContent = `updated ${ageSec}s ago`;
      meta.append(name, sub);

      const tag = document.createElement('span');
      tag.className = 'tag';
      tag.textContent = u.source;

      li.append(avatar, meta, tag);
      li.addEventListener('click', () => {
        mapView.focus(u.userId);
        $('panel').classList.remove('is-open');
      });
      list.append(li);
    }
  }

  function renderStats() {
    $('stat-sent').textContent = stats.sent;
    $('stat-acked').textContent = stats.acked;
    $('stat-dupes').textContent = stats.dupes;
    $('stat-rejected').textContent = stats.rejected;
  }

  function setConnStatus(kind, text) {
    const el = $('conn-status');
    el.className = `pill pill--${kind}`;
    el.textContent = text;
  }

  function setPermissionText(state, message) {
    const labels = {
      granted: 'Location permission: granted ✅',
      prompt: 'Location permission: will be asked when you start sharing',
      denied: 'Location permission: denied ❌ (allow it in site settings, or simulate)',
      error: `Location error: ${message ?? 'unknown'}`,
      unknown: 'Location permission: unknown',
    };
    $('permission-status').textContent = labels[state] ?? state;
  }

  $('panel-toggle').addEventListener('click', () => $('panel').classList.toggle('is-open'));

  // Fade / drop quiet markers and refresh "updated Xs ago".
  setInterval(() => {
    mapView.sweep(staleAfterMs);
    renderUserList();
  }, 2_000);
  renderUserList();
}

function log(kind, text) {
  const li = document.createElement('li');
  li.className = kind;
  li.textContent = `${new Date().toLocaleTimeString()}  ${text}`;
  const list = $('event-log');
  list.prepend(li);
  while (list.children.length > 40) list.lastChild.remove();
}
