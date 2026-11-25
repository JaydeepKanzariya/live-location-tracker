// Everything Leaflet: one marker + short trail per user, smooth movement, stale fading.
/* global L */

const TRAIL_POINTS = 30;

export function colorFor(userId) {
  let hash = 0;
  for (const ch of userId) hash = (hash * 31 + ch.charCodeAt(0)) | 0;
  // Golden-angle spacing so similar ids still get clearly different hues.
  return `hsl(${Math.round(Math.abs(hash) * 137.508) % 360} 65% 45%)`;
}

export function initials(name) {
  return name.split(/\s+/).filter(Boolean).slice(0, 2).map((w) => w[0].toUpperCase()).join('') || '?';
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
}

export class MapView {
  constructor(elementId) {
    this.map = L.map(elementId, { zoomControl: true }).setView([20.5937, 78.9629], 5);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap contributors',
    }).addTo(this.map);
    this.entries = new Map(); // userId -> { marker, trail, data, animation }
    this.myUserId = null;
    this.hasCenteredOnMe = false;
    this.historyLayer = null;
  }

  setMyUserId(id) {
    this.myUserId = id;
  }

  /** Create or move a user's marker from a `location:broadcast` payload. */
  upsert(data) {
    const isMe = data.userId === this.myUserId;
    const target = L.latLng(data.lat, data.lng);
    let entry = this.entries.get(data.userId);

    if (!entry) {
      const icon = L.divIcon({
        className: '',
        iconSize: [34, 34],
        iconAnchor: [17, 17],
        html: `<div class="pin ${isMe ? 'pin--me' : ''}" style="--c:${colorFor(data.userId)}">${escapeHtml(initials(data.displayName))}</div>`,
      });
      const marker = L.marker(target, { icon, zIndexOffset: isMe ? 1000 : 0 })
        .bindTooltip(isMe ? 'You' : escapeHtml(data.displayName), {
          permanent: true, direction: 'top', offset: [0, -18], className: 'name-label',
        })
        .addTo(this.map);
      const trail = L.polyline([target], { color: colorFor(data.userId), weight: 3, opacity: 0.6 }).addTo(this.map);
      entry = { marker, trail, data, animation: null };
      this.entries.set(data.userId, entry);
    } else {
      this.animateTo(entry, target);
      const points = entry.trail.getLatLngs();
      points.push(target);
      if (points.length > TRAIL_POINTS) points.shift();
      entry.trail.setLatLngs(points);
    }

    entry.data = data;
    entry.receivedAt = Date.now();
    entry.marker.getElement()?.querySelector('.pin')?.classList.remove('pin--stale');
    if (entry.marker.getPopup()) entry.marker.setPopupContent(this.popupHtml(data));
    else entry.marker.bindPopup(this.popupHtml(data));

    if (isMe && !this.hasCenteredOnMe) {
      this.map.setView(target, 16);
      this.hasCenteredOnMe = true;
    }
  }

  popupHtml(d) {
    const acc = d.accuracy != null ? `±${Math.round(d.accuracy)} m` : 'n/a';
    return `<strong>${escapeHtml(d.displayName)}</strong><br>
      ${d.lat.toFixed(5)}, ${d.lng.toFixed(5)} (${acc})<br>
      source: ${escapeHtml(d.source)}<br>
      kafka: partition ${d.kafka?.partition} · offset ${d.kafka?.offset}`;
  }

  /** Glide the marker to its new position instead of teleporting. */
  animateTo(entry, target) {
    cancelAnimationFrame(entry.animation);
    const from = entry.marker.getLatLng();
    const duration = 800;
    const start = performance.now();
    const tick = (now) => {
      const t = Math.min((now - start) / duration, 1);
      const eased = t * (2 - t);
      entry.marker.setLatLng([from.lat + (target.lat - from.lat) * eased, from.lng + (target.lng - from.lng) * eased]);
      if (t < 1) entry.animation = requestAnimationFrame(tick);
    };
    entry.animation = requestAnimationFrame(tick);
  }

  remove(userId) {
    const entry = this.entries.get(userId);
    if (!entry) return;
    cancelAnimationFrame(entry.animation);
    entry.marker.remove();
    entry.trail.remove();
    this.entries.delete(userId);
  }

  /**
   * Client-side safety net: fade markers that went quiet, drop them if the server's
   * `user:offline` was missed (e.g. we were disconnected when it was sent).
   */
  sweep(staleAfterMs) {
    const now = Date.now();
    for (const [userId, entry] of this.entries) {
      const age = now - entry.receivedAt;
      if (age > staleAfterMs + 5_000) this.remove(userId);
      else if (age > staleAfterMs / 2) entry.marker.getElement()?.querySelector('.pin')?.classList.add('pin--stale');
    }
  }

  focus(userId) {
    const entry = this.entries.get(userId);
    if (entry) {
      this.map.flyTo(entry.marker.getLatLng(), Math.max(this.map.getZoom(), 16));
      entry.marker.openPopup();
    }
  }

  centerOn(lat, lng) {
    if (!this.hasCenteredOnMe) {
      this.map.setView([lat, lng], 16);
      this.hasCenteredOnMe = true;
    }
  }

  /** Draw (or hide) the user's persisted trail loaded from /api/history. */
  toggleHistory(points) {
    if (this.historyLayer) {
      this.historyLayer.remove();
      this.historyLayer = null;
      return false;
    }
    const latLngs = points.map((p) => [p.lat, p.lng]);
    this.historyLayer = L.layerGroup([
      L.polyline(latLngs, { color: '#facc15', weight: 4, dashArray: '6 8' }),
      ...latLngs.map((ll) => L.circleMarker(ll, { radius: 3, color: '#facc15', fillOpacity: 1 })),
    ]).addTo(this.map);
    if (latLngs.length) this.map.fitBounds(latLngs, { padding: [40, 40] });
    return true;
  }

  list() {
    return [...this.entries.values()].map((e) => ({ ...e.data, receivedAt: e.receivedAt }));
  }
}
