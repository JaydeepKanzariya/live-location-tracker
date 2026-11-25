# 08 · Frontend (React 19 + Vite + Leaflet)

## Structure (`apps/web`)

```
src/
  app/            routes (TanStack Router or React Router 7), providers, error boundary
  features/
    auth/         useMe(), <RequireAuth>, login screen
    sharing/      useGeolocation(), useLocationSender(), <SharingPanel>
    map/          <LiveMap>, MarkerLayer (imperative), TrailLayer, HistoryLayer, useViewportSync()
    presence/     presenceStore (Zustand), <UserList>
    diagnostics/  <EventLog>, <KafkaBadge> (partition/offset), connection status
  lib/
    socket.ts     typed socket.io-client singleton (contracts types)
    api.ts        fetch wrapper + TanStack Query client
  styles/         Tailwind, shadcn/ui components
```

## State model

| State | Where | Why |
|---|---|---|
| Server data (me, history) | **TanStack Query** | caching, retries, loading/error states |
| Live presence (`Map<userId, Entry>`) | **Zustand** store outside React render, updated by socket handlers | 100s of updates/s must not re-render the tree |
| Map objects (Leaflet markers) | **refs**, updated imperatively by subscribing to the store | React reconciliation per marker per update is too slow |
| UI prefs (interval, panel open) | Zustand + `localStorage` | survives reload |

**Rule:** React renders the *shell* (panels, list, controls). Leaflet layers are driven **imperatively** from the store. This is how high-frequency map apps avoid jank.

## Map rendering

- **react-leaflet** for the `MapContainer` and tile layer; a custom `MarkerLayer` component owns a `Map<userId, L.Marker>` and diffs against the store.
- **Smooth movement:** interpolate between positions with `requestAnimationFrame` over ~the send interval (dead reckoning with heading/speed is a COULD).
- **Many users:** switch to `L.canvas()` circle markers above ~300 markers; **supercluster** for clustering when zoomed out.
- **Me vs others:** "me" = pulsing ring + accuracy circle + always on top; others get a deterministic colour (golden-angle hue from `sub`), initials and a name label; stale (> 25 s) = dimmed to 40% with an "Idle" tag.
- **Trails:** last 30 points per user as a polyline; own stored history as a dashed GeoJSON layer from `/api/me/history`.
- **Tiles:** OSM for development; **MapTiler/Stadia** key in production (OSM's tile policy forbids heavy use).

## UX states (each one designed and tested)

| State | UI |
|---|---|
| Signed out | landing + "Sign in" |
| Permission `prompt` | explainer card before triggering the browser prompt (better grant rates) |
| Permission `denied` | how-to-enable instructions per browser + "Simulate walking" fallback |
| Position unavailable / timeout | inline warning, keeps retrying |
| Socket connecting / reconnecting / offline | status pill; sharing auto-resumes on reconnect |
| Server rejected update | toast with reason (rate limited / invalid) |
| Session expired | modal → re-login, returns to the same view |
| Nobody else online | empty state with "open another window as bob" hint (demo-friendly) |

## Quality bar
- **Accessibility:** keyboard-navigable panel, user list mirrors the map for screen readers, colour is never the only signal, WCAG AA contrast.
- **Responsive:** panel becomes a bottom sheet under 768 px; 44 px touch targets.
- **Performance budget:** JS < 200 KB gzip initial; first meaningful map paint < 2 s on 4G; 60 fps with 500 moving markers.
- **Resilience:** error boundary per feature; the map still works if the history API fails.

---

# Part 2 · UI/UX Specification: Tactical Map HUD

> Added 2026-11-25. Part 1 above is the architecture; this part is the visual and interaction spec. Where the two disagree, **this part wins** (see [Reconciliation](#reconciliation-with-other-plan-documents)). **Part 3 overrides this part on colours, panel styling and marker animation.**
> Recommended Claude Code skills when building it: `anthropic-frontend-design` + `impeccable`.

## 1. Design direction

The UI is a **full-viewport operational dispatch console** (`100vw × 100vh`), not a multi-page SaaS dashboard.

- **Canvas-first layout:** the Leaflet map fills the viewport (`fixed inset-0`). All controls float above it as translucent `shadcn/ui` cards (`backdrop-blur-md bg-slate-950/80 border-slate-800/80`).
- **Typography:** `Inter` / `Geist` for labels, `JetBrains Mono` with `tabular-nums` for coordinates, timestamps and latency, so numbers don't jitter when they update every 3 s.
- **Colour semantics:**
  - **You:** electric blue `#3B82F6`, pulse beacon and accuracy radius
  - **Active peers:** emerald `#10B981` or amber `#F59E0B` pill markers
  - **Stale peers (> 25 s without heartbeat):** 40% opacity with an `Idle` tag, removed at 35 s
  - **Pipeline health:** emerald = Connected, amber = Reconnecting, rose = Disconnected / Auth required

## 2. Folder structure (`apps/web/src`)

```text
apps/web/src/
├── api/
│   ├── auth.ts                  # BFF session fetch (/api/auth/me, /auth/login, /auth/logout)
│   └── history.ts               # TanStack Query hooks for GET /api/locations/history
├── components/
│   ├── hud/
│   │   ├── TopStatusBar.tsx     # app badge, socket status, e2e latency pill, share toggle, profile
│   │   ├── ActiveUsersDrawer.tsx # collapsible list of live sharers + flyTo action
│   │   ├── MapControls.tsx      # centre on me, map theme, history trail, dev simulator
│   │   └── PermissionBanner.tsx # non-blocking geolocation prompt / denied recovery alert
│   ├── map/
│   │   ├── LiveMap.tsx          # MapContainer wrapper + tile layer switcher
│   │   ├── UserMarker.tsx       # memoised L.divIcon marker with smooth CSS glide
│   │   ├── HistoryTrail.tsx     # Polyline + start/end markers for the user's own trail
│   │   └── StaleSweepEffect.tsx # interval cleanup removing users inactive > 35 s
│   └── auth/
│       └── LoginOverlay.tsx     # floating OIDC sign-in card over a blurred preview map
├── hooks/
│   ├── useSocketBridge.ts       # connects Socket.IO with BFF credentials, validates Zod events
│   └── useGeolocationStream.ts  # watchPosition + 3 s throttle + heartbeat
├── store/
│   └── usePresenceStore.ts      # Zustand store keyed by userId (avoids full-map re-renders)
├── App.tsx
└── main.tsx
```

This replaces the shorter `features/` layout in Part 1. The state model and rendering rules in Part 1 still apply.

## 3. UI states and user flows

### State A: Unauthenticated gate (F1, F2)
- **Visual:** the live map renders behind a full-screen `backdrop-blur-sm bg-slate-950/60` scrim, centred on a default city (e.g. Ahmedabad `23.0225, 72.5714`).
- **Card:** project title, one line (*"Authenticated real-time telemetry powered by OIDC, Socket.IO & Kafka"*), and a primary **Sign in with OIDC** button to `/auth/login`.
- **Security guarantee:** the Socket.IO client is **not initialised** until `GET /api/auth/me` returns a valid session with `sub`.

### State B: Geolocation permission and streaming (F3, F4)
- **Toggle:** the user explicitly switches **Share Live Location** in the `TopStatusBar`.
- **Permission states:**
  - `prompt`: subtle top-centre banner explaining why location is needed
  - `denied`: amber recovery card with instructions to re-enable location in site settings; read-only viewing of other users still works
  - `unavailable`: graceful message with an option to enable the **Dev Route Simulator**
- **Emission cadence (`useGeolocationStream`):**
  - at most one `location:update` every **3,000 ms** while moving (distance > 3 m)
  - a **heartbeat every 15,000 ms** when stationary, so the server and peers don't mark the user stale at 35 s

### State C: Live map and peer tracking (F8, F9)
- **Current user:** electric-blue beacon with a CSS `@keyframes` pulse ring and a translucent `L.Circle` showing GPS `accuracy` in metres.
- **Other users:** compact pill with initials/name and a direction arrow when `heading` is available.
- **Smooth movement (no teleporting):** apply `transition: transform 1.2s linear` to `.leaflet-marker-icon`, and disable the transition between `zoomstart` and `zoomend` so markers glide between 3 s updates without drifting during zoom.
- **Stale handling:** every 2,000 ms `StaleSweepEffect` checks `Date.now() - user.lastSeenAt`:
  - `> 25,000 ms`: `isStale = true` (40% opacity + `Idle` tag)
  - `> 35,000 ms`, or on a `user:offline` socket event: remove from the Zustand store and unmount the marker

### State D: Own history trail (F10, SHOULD)
- **Control:** **My History Trail** toggle in `MapControls`.
- **Data:** TanStack Query calls `GET /api/locations/history?window=30m`, authenticated by the HttpOnly session cookie; the backend enforces `userId === session.sub`.
- **Rendering:** dashed electric-blue `Polyline` (`weight={3}`, `dashArray="6, 8"`) with timestamp tooltips on hover.

## 4. State management and performance rules

1. **Per-user Zustand subscriptions (`usePresenceStore`):** keep peers in a `Record<string, PeerLocation>`, not an array. `LiveMap` subscribes only to `Object.keys(peers)` and re-renders only when someone joins or leaves. Each `<UserMarker userId={id} />` subscribes only to `state.peers[id]`.
2. **Client-side dedupe and ordering:** ignore an incoming event when `event.timestamp <= existingPeer.timestamp` or `event.eventId === existingPeer.lastEventId`.
3. **Memoised `L.divIcon`:** depend only on `[userId, displayName, isCurrentUser, isStale]`. Never include `lat`/`lng`, or Leaflet recreates the DOM node on every update.
4. **End-to-end latency telemetry:** `latencyMs = Date.now() - event.clientTimestamp`, averaged over the last 10 events, shown in the `TopStatusBar` as live proof of the `Socket.IO → Kafka → consumer → Socket.IO` path.

## 5. Demo and testing helpers (built into the UI)

- **Dev Route Simulator (`VITE_ENABLE_SIMULATOR=true`):** a collapsible *Simulate Movement* pill in `MapControls` that walks the current user around a loop, emitting real `location:update` events through the actual Socket.IO → Kafka pipeline.
- **Two-window clarity:** show the signed-in user's `name` and a truncated `sub` in the `TopStatusBar`, so a normal window and an incognito window are easy to tell apart in the recording.

## 6. Acceptance checklist (rubric alignment)

- [ ] Unauthenticated users see the login gate and cannot open a Socket.IO connection (15 marks: OIDC)
- [ ] The map loads cleanly with dark/light tiles, and "You" is clearly different from other users (10 marks: Frontend map)
- [ ] Marker positions glide when Kafka-backed updates arrive (20 marks: Core functionality)
- [ ] Disconnected or idle users (> 35 s) fade and disappear from every map (15 marks: WebSocket)
- [ ] Out-of-order or duplicate events are ignored on the client (20 marks: Core functionality)
- [ ] The UI shows connection status and e2e latency to demonstrate the Kafka pipeline in the video (10 marks: Demo video)

## Reconciliation with other plan documents

These numbers and names differ between this part and other plan files. This part is the intended value; the other files need a matching edit when v1 is built.

| Item | This part | Elsewhere |
|---|---|---|
| Heartbeat when stationary | 15 s | [05](05-realtime-gateway.md) says 10 s |
| Server stale threshold | client removes at 35 s | [01](01-requirements.md) and [07](07-data-storage.md) remove on the server at 30 s (the client rule is the backstop) |
| History endpoint | `GET /api/locations/history?window=30m` | [07](07-data-storage.md) uses `GET /api/me/history?from&to&limit` |
| Session endpoint | `/api/auth/me` | prototype uses `/api/me` |

Note that a 15 s heartbeat against a 30 s server threshold tolerates only one missed heartbeat. If that proves flaky in testing, lower the heartbeat to 10 s rather than raising the threshold.

---

# Part 3 · Visual Design System: "Signal"

> Added 2026-11-25, revised after review of the "floating islands" notes (same day). This is the final visual language. **Part 3 overrides Part 2** on colours, panel styling and marker animation. Part 2 still defines the screens, states and behaviour. The same rules are in the project-level [`DESIGN.md`](../../DESIGN.md), which is what a coding agent should read first.

## 1. Direction

**Dark mission control with one warm signal colour.** The reference feel is Linear / Vercel / Raycast: near-black surfaces, 1 px hairline borders, a single accent, a faint noise texture. The login page borrows one idea from editorial design: a large serif headline over a drifting live map.

Why this and not the Part 2 look (slate glass + blue / emerald / amber):

| Part 2 default | Part 3 replacement | Reason |
|---|---|---|
| Frosted glass on every card | **Glass only on three small floating pieces** (status pill, avatar stack, action dock); large surfaces (expanded user list, phone bottom sheet) stay ~92% solid | Glass everywhere is the most recognisable "generated UI" tell, and large blurred areas over a moving map cost GPU and hurt text contrast |
| Blue + emerald + amber together | **One accent** (signal orange); green/amber/red only for connection state | Three equal colours means nothing stands out |
| Default OSM tiles | **Muted CARTO basemap** (dark / light) | The basemap is ~80% of the screen; default tiles read as "Leaflet tutorial" |
| CSS `transition` on `.leaflet-marker-icon` | **rAF interpolation** in code | Leaflet already positions markers with `transform`; a CSS transition on it fights zoom animation |
| Full-width top bar | **Floating "island" pill** that changes shape with state | Keeps the map as the hero |
| Full-height left sidebar | **Avatar stack** that expands into a floating card | Nothing chops the map into a small box |

## 2. Colour tokens

Colours live in CSS variables; light and dark swap **tokens, not components**.

```css
:root {                      /* dark is the default */
  --bg:          #0A0B0D;    /* page / behind map controls */
  --surface:     #101215;    /* panels */
  --surface-2:   #16181C;    /* rows, inputs, hover */
  --border:      #23262B;    /* hairline */
  --border-2:    #2F333A;    /* hover / focus-adjacent */
  --text:        #ECEDEF;
  --text-muted:  #9097A1;    /* secondary text (≈6.7:1 on --bg) */
  --text-faint:  #626872;    /* decorative / disabled ONLY (≈3.6:1, below AA for body text) */

  --accent:      #FF7A45;    /* "signal": you, primary action, live pulse (≈7.7:1 on --bg) */
  --accent-ink:  #0A0B0D;    /* text on accent */
  --accent-rgb:  255 122 69;

  --ok:   #3DD68C;           /* connected, live */
  --warn: #F5A524;           /* reconnecting, idle */
  --bad:  #F0524F;           /* disconnected, error */
}
:root[data-theme="light"] {
  --bg: #F6F5F2;  --surface: #FFFFFF;  --surface-2: #F0EEE9;
  --border: #E4E2DC;  --border-2: #D3D0C8;
  --text: #14151A;  --text-muted: #565B66;  --text-faint: #8A8F99;
  --accent: #C2410C;  --accent-ink: #FFFFFF;  --accent-rgb: 194 65 12;   /* ≈5.2:1 with white text */
  --ok: #15803D;  --warn: #B45309;  --bad: #B91C1C;
}
```

**Peer colours:** other users get one of **7 desaturated hues** picked deterministically from `sub`, used only for the avatar ring and trail. Orange is reserved for "you", so it never appears on a peer.

```css
--peer-hues: 95 140 180 220 260 300 340;     /* hue = pick[hash(sub) % 7] */
/* ring colour: oklch(72% 0.12 <hue>)  (dark)   oklch(55% 0.13 <hue>)  (light) */
```

Verify every pairing with a contrast checker before shipping. The ratios above are calculated, not measured on the final UI.

## 3. Typography

| Role | Font | Notes |
|---|---|---|
| UI text | **Geist** | 400 / 500 / 600 |
| Numbers, coordinates, latency, IDs | **Geist Mono** | always `font-variant-numeric: tabular-nums` |
| Login headline only | **Instrument Serif** (regular + italic) | one use, large, e.g. *"Watch it move."* |

App scale (fixed, because it is a tool UI, not a marketing page): **11 / 12 / 13 / 14 / 16 / 20 / 28 px**. Body 13–14 px; panel titles 12 px uppercase with `letter-spacing: .06em` in `--text-muted`; login headline `clamp(2.5rem, 1.5rem + 5vw, 5rem)` with `letter-spacing: -0.03em` and `text-wrap: balance`.

## 4. Layout system

- **Spacing:** 4 px base (4 / 8 / 12 / 16 / 24 / 32). Every floating element sits **16 px** from the viewport edge (12 px on phones).
- **Radius:** controls 10 px · panels 16 px · pills and avatars full. Nested radius = outer − padding.
- **Desktop:**
  - **Status island** (top-centre, rounded-full, ~44 px high, **glass**): connection dot · latency ticker · "Share live location" toggle · avatar menu. It **morphs** (Motion `layout`, spring below) when state changes: it grows slightly while *Reconnecting* or *Acquiring GPS*, and expands to show speed and accuracy while *Live sharing* is on.
  - **Avatar stack** (left, **glass**): collapsed by default to overlapping avatars plus "+4 live". Click to expand into a floating card (not full height, max 60% of the viewport height, **~92% solid**) with *You* → *On the map* list → *Event log* (collapsed by default, expanded for the demo).
  - **Action dock** (bottom-right, **glass**): re-centre / follow-me, theme, history trail, simulator. A vertical group of 40 px icon buttons.
- **Phone (< 768 px):** top pill shrinks to dot + toggle; the left panel becomes a **draggable bottom sheet** with three snap points (peek 96 px · half · full); controls move to the right edge above the sheet. All touch targets ≥ 44 px.
- **Command palette:** `⌘K` / `Ctrl+K` opens a centred search to jump to a user, toggle sharing or switch theme.

## 5. Depth and texture

- **Two surface types, used deliberately:**
  - **Glass** (status island, avatar stack, action dock only): `background: rgb(16 18 21 / .6); backdrop-filter: blur(20px) saturate(140%); box-shadow: 0 0 0 1px rgb(255 255 255 / .10) (the "ring-1 ring-white/10" hairline), 0 24px 48px -16px rgb(0 0 0 / .6);`. The map colours bleed through softly.
  - **Solid** (expanded user list, phone bottom sheet, modals, toasts): `--surface` at ~92% opacity, 1 px `--border`, and the panel shadow below.
  ```css
  --shadow-panel: 0 1px 0 rgb(255 255 255 / .04) inset, 0 12px 32px -12px rgb(0 0 0 / .55);
  ```
  Never blur a large area. Glass is for small floating pieces only.
- **Noise:** one fixed SVG `feTurbulence` layer at 3–4% opacity over the app chrome (not over the map), `pointer-events: none`.
- **Spotlight:** panel rows get a faint radial glow that follows the pointer (`--mx` / `--my`), using `rgb(var(--accent-rgb) / .08)`. Desktop only; off for `prefers-reduced-motion`.

## 6. Map styling

- **Basemap:** CARTO *Dark Matter* in dark theme and CARTO *Voyager* in light theme, switched with the theme token (cross-fade 250 ms). Use the `*_nolabels` variants plus a separate labels layer on top if you want labels to stay above trails.
  ```ts
  const tiles = {
    dark:  'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
    light: 'https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png',
  };
  // attribution: '© OpenStreetMap contributors © CARTO'
  ```
  Check CARTO's current usage terms before production use. Stadia *Alidade Smooth Dark* is a good alternative but needs an account and API key outside localhost; MapTiler is another keyed option for high-traffic deployments.
- **Reduce tile contrast:** `filter: saturate(.85) brightness(.95)` on the tile pane in dark mode so markers carry the colour.
- **Marker anatomy:**
  - *Peer:* 32 px circle, initials, 2 px ring in the peer hue, small heading cone when `heading` exists, name label appears on hover/focus (not permanently, to avoid clutter).
  - *You:* 36 px circle filled with `--accent`, `--accent-ink` initials, a **dual-ring radar ripple** (two rings offset by 1.2 s, 2.4 s each, `ease-out`, scale 1 → 2.2, opacity .5 → 0), a translucent accuracy circle, and a **directional cone** rotated by `heading` (`transform: rotate(<deg>)` on an inner element). The cone is hidden when `heading` is `null` (standing still, desktop), and rotation takes the **shortest angle** (350° → 10° turns 20°, not 340°).
  - *Stale (> 25 s):* grayscale, 40% opacity, "Idle" chip.
  - *Cluster (zoomed out):* count bubble; on zoom-in it splits with a 250 ms spring.
- **Trails:** last 30 points, 2 px line, peer hue at 60% opacity fading to 0 at the tail (gradient via segment opacity).
- **History breadcrumb (my stored trail):** when toggled on, the path **draws itself** start to finish, then settles into the dashed style. Use Leaflet's SVG renderer: read the path element (`polyline.getElement()`), set `stroke-dasharray = stroke-dashoffset = pathLength`, then transition `stroke-dashoffset` to 0 over ~1.2 s (`--ease-move`). Animate **once per toggle**, not on zoom (path length changes with zoom), and not with the canvas renderer (no per-path DOM element).

## 6.1 Camera behaviour and telemetry

**Follow-me camera lock**
- While sharing, the map keeps *you* centred (`map.panTo`, 400 ms) on each new fix.
- The lock **releases the moment the user takes control**: a real `mousedown` / `wheel` / `touchstart` on the map, then `dragstart` or `zoomstart`. A small **Re-center** pill (glass) appears near the dock; clicking it re-locks and flies back.
- Programmatic moves (`flyTo`, `panTo`, `fitBounds`) also fire `movestart` / `zoomstart`, so keep an `isProgrammaticMove` flag set around our own calls and ignore those events; otherwise the camera would unlock itself.
- Clicking another user in the list also releases the lock, and flies to them with `map.flyTo([lat, lng], 16, { duration: 1.2 })`, never an instant `setView`.

**Human-readable telemetry** (in the island when sharing, and in marker popups)
| Value | Format | Rule |
|---|---|---|
| Speed | `14 km/h` (`speedMps × 3.6`, rounded) | hidden when `speed` is `null` or below 1 km/h |
| Accuracy | badge `±8 m GPS` | amber badge above 50 m, "Approximate" label above 200 m |
| Freshness | `2s ago`, updating every second | uses `receivedAt`, not the device clock |
| Coordinates | `19.07600, 72.87770` in Geist Mono | **Copy** button → toast "Coordinates copied" |

Copy-coordinates is offered for **your own** position and for any user currently on the map (every signed-in user can already see them); it never exposes stored history.

## 7. Motion system

One family, used everywhere. Animate only `transform`, `opacity`, `filter`.

```css
:root {
  --dur-instant: 80ms;   --dur-fast: 150ms;   --dur-base: 250ms;   --dur-slow: 400ms;
  --ease-out: cubic-bezier(.16, 1, .3, 1);       /* entering */
  --ease-in:  cubic-bezier(.7, 0, .84, 0);       /* leaving (≈25% faster than entering) */
  --ease-move: cubic-bezier(.65, 0, .35, 1);     /* A → B on screen */
  --stagger: 60ms;
}
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation-duration: 1ms !important; transition-duration: 1ms !important; }
}
```

| Moment | Animation | Spec |
|---|---|---|
| **Marker move** | interpolate lat/lng in code | `requestAnimationFrame`, duration = min(send interval, 1500 ms), ease-out; cancel and restart when a newer fix arrives |
| **User joins / leaves** | marker scales .6 → 1 and fades; leaving fades + shrinks | 250 ms in / 180 ms out |
| **List row** | enter with stagger, re-order smoothly | Motion `layout` + `AnimatePresence`, spring `{ stiffness: 260, damping: 26 }` |
| **HUD panels** (island morph, avatar stack expand, history panel) | spring | Motion `{ type: "spring", stiffness: 380, damping: 30 }` |
| **Click a user** | `map.flyTo(..., 16, { duration: 1.2 })`, `easeLinearity .25`; popup scales from the row's position; follow-me lock releases | `transform-origin` = trigger |
| **My history trail** | path draws itself (`stroke-dashoffset` → 0), then dashed | ~1.2 s, `--ease-move`, once per toggle |
| **Your marker** | dual radar ripple + cone rotation (shortest angle) | ripple 2.4 s loop; cone 250 ms `--ease-out` |
| **Login → app** | blurred map sharpens, panels slide in 12 px + fade, stagger 60 ms | View Transitions API with a `motion` fallback |
| **Latency pill** | number ticks (spring count), dot "breathes" while connected | dot: 2 s `ease-in-out` opacity .5 ↔ 1 |
| **Share toggle on** | accent fills left→right, pulse on your marker starts | 250 ms |
| **Loading** | shimmer skeletons for the user list and history | CSS gradient shimmer, 1.4 s |
| **Events** | toast bottom-centre: "Bob went offline" | slide-up 12 px, auto-dismiss 4 s |
| **Press feedback** | buttons scale .97 | 80 ms |

Cap list staggers at ~600 ms total. Never animate `width`, `height`, `top` or `left`.

```ts
// marker glide (replaces CSS transition on .leaflet-marker-icon)
function glide(marker: L.Marker, to: L.LatLng, ms: number) {
  const from = marker.getLatLng(); const t0 = performance.now();
  cancelAnimationFrame(marker._raf);
  const step = (now: number) => {
    const t = Math.min((now - t0) / ms, 1), e = 1 - Math.pow(1 - t, 3);   // ease-out cubic
    marker.setLatLng([from.lat + (to.lat - from.lat) * e, from.lng + (to.lng - from.lng) * e]);
    if (t < 1) marker._raf = requestAnimationFrame(step);
  };
  marker._raf = requestAnimationFrame(step);
}
```

## 8. Component inventory

| Component | Built from | Notes |
|---|---|---|
| Top pill, buttons, toggle, menu, tooltip | shadcn/ui (Radix) restyled with the tokens above | do not ship default shadcn look: change radius, borders, focus ring (2 px accent, 2 px offset) |
| Bottom sheet (phone) | `vaul` | three snap points |
| Command palette | `cmdk` | `⌘K` |
| Toasts | `sonner` | styled to tokens |
| Animations | `motion` | layout + presence + springs |
| Icons | `lucide-react` | one set only, 1.5 px stroke, 16 / 20 px |
| Fonts | `geist` package + Instrument Serif (Fontsource) | self-hosted, `font-display: swap` |

Check current versions of every package before installing.

## 9. Login page

- Full-viewport map behind, dark basemap, centred on the default city, **drifting simulated dots** (6–10 fake peers on slow loops, no network needed).
- Left-aligned card, not centred: serif headline *"Watch it move."*, one sentence of real copy, one **Sign in** button (accent), and a small "How it works: Socket.IO → Kafka → map" line with the three words as quiet chips.
- No illustration, no gradient text, no emoji.

## 10. Empty, error and edge states (each designed)

| State | Treatment |
|---|---|
| Nobody else online | the status island and avatar stack show an inline hint: "You're the first one here. Open a second window and sign in as another user to see live tracking." (no "share link": the app has no invite links), plus a *Copy login hint* button |
| Permission denied | inline card in the panel (not a modal), step list per browser, secondary button *Use simulator* |
| Offline / reconnecting | pill dot turns amber and breathes, map stays interactive, toast on recovery |
| Session expired | modal with one action, returns to the same view after login |
| History empty | "No stored points in the last 30 min." with a hint that downsampling keeps ~1 point per 10 m / 30 s |

## 11. Accessibility

- Text contrast ≥ 4.5:1 (`--text-faint` is never used for readable text); visible 2 px focus ring on every control.
- Everything on the map is also in the left list, so keyboard and screen-reader users can reach every user; Enter on a row flies to it.
- Status is never colour-only: connection pill includes the word *Live / Reconnecting / Offline*.
- `prefers-reduced-motion`: no pulse, no glide (markers jump), no spotlight; toasts fade only.
- Glass fallback: `@supports not (backdrop-filter: blur(1px))` gives the three glass elements a solid `--surface`. Text on glass must still pass 4.5:1 over both light and dark map areas (test over water, parks and roads).
- The follow-me lock never fights the user: any manual pan or zoom releases it, and the Re-center pill is keyboard reachable.

## 12. "Not made by a template" checklist

- [ ] Only one accent colour is visible on a normal screen
- [ ] Blur is used only on the three small glass pieces (status island, avatar stack, action dock)
- [ ] No full-height sidebar and no full-width navbar anywhere
- [ ] Panning or zooming the map never gets undone by the follow-me camera
- [ ] Real copy everywhere ("Share live location", never "Enable feature")
- [ ] Cards are not all the same size and radius; hierarchy comes from spacing and weight
- [ ] One icon set, no emoji in the UI
- [ ] Map tiles are not default OSM
- [ ] Every state in section 10 has been designed, not left as a blank
- [ ] Animations all use the shared tokens in section 7

## Reconciliation with Part 2

| Part 2 says | Part 3 says (wins) |
|---|---|
| Electric blue "you", emerald / amber peers | Orange accent for "you"; peers use 7 muted hues |
| Translucent glass cards | Glass on three small pieces only; big surfaces ~92% solid |
| `TopStatusBar` full-width | Floating status island that morphs with state |
| CSS `transition: transform 1.2s linear` on markers | rAF glide, ease-out, ≤ 1.5 s |
| OSM tiles | CARTO Dark Matter / Voyager |
| `ActiveUsersDrawer` | Avatar stack that expands into a floating card, and a bottom sheet on phones |
| Slate-950 neutral palette | Near-black neutral tokens in section 2 |

Behaviour in Part 2 (states A to D, heartbeat, stale thresholds, store rules, simulator) is unchanged.
