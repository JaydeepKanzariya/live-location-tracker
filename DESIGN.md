# DESIGN.md · Live Location Tracker ("Signal")

> Project-level design spec. If anything here conflicts with a general guideline, this file wins. Full rationale, screens and component details are in `docs/plan/08-frontend.md` (Part 2 = behaviour, Part 3 = visual system).

## Overview

A dark, quiet mission-control console for watching people move on a live map. The map is the hero; everything else is small, solid and calm. One warm accent ("signal orange") marks *you* and the primary action. Feel: Linear / Raycast, with a serif headline on the login page only.

## Colors

Tokens only; never hard-code a hex in a component. Dark is default.

| Token | Dark | Light |
|---|---|---|
| `--bg` | `#0A0B0D` | `#F6F5F2` |
| `--surface` | `#101215` | `#FFFFFF` |
| `--surface-2` | `#16181C` | `#F0EEE9` |
| `--border` / `--border-2` | `#23262B` / `#2F333A` | `#E4E2DC` / `#D3D0C8` |
| `--text` | `#ECEDEF` | `#14151A` |
| `--text-muted` | `#9097A1` | `#565B66` |
| `--text-faint` (decorative only) | `#626872` | `#8A8F99` |
| `--accent` / `--accent-ink` | `#FF7A45` / `#0A0B0D` | `#C2410C` / `#FFFFFF` |
| `--ok` / `--warn` / `--bad` | `#3DD68C` / `#F5A524` / `#F0524F` | `#15803D` / `#B45309` / `#B91C1C` |

Peers use 7 desaturated hues (95, 140, 180, 220, 260, 300, 340) via `oklch`, chosen from a hash of the user id. Orange is never used for a peer.

## Typography

- **Geist** for UI, **Geist Mono** (tabular-nums) for numbers, coordinates, latency and ids, **Instrument Serif** for the login headline only.
- Scale 11 / 12 / 13 / 14 / 16 / 20 / 28 px. Body 13 to 14 px. Panel titles 12 px uppercase, tracking .06em, `--text-muted`.

## Layout

- 4 px spacing base. Floating elements sit 16 px from the viewport edge (12 px on phones).
- No full-height sidebar and no full-width navbar. Three floating "islands" over an edge-to-edge map (`100dvh`):
  - **Status island** (top-centre, rounded-full): connection dot, latency, share toggle, avatar menu. It morphs with state (grows when reconnecting or acquiring GPS, shows speed and accuracy while sharing).
  - **Avatar stack** (left): collapsed to overlapping avatars plus "+4 live"; click to expand into a floating card (max 60% viewport height).
  - **Action dock** (bottom-right): re-centre / follow-me, theme, history trail, simulator.
- Phone: the expanded list becomes a bottom sheet with peek / half / full snap points; touch targets 44 px or more.
- `Ctrl/Cmd+K` command palette.
- **Follow-me camera:** keeps you centred while sharing, releases the moment the user pans or zooms (ignore our own programmatic moves), and shows a "Re-center" pill. Clicking a user flies there with `flyTo(..., 16, { duration: 1.2 })`.
- Telemetry shown as speed (`14 km/h`), accuracy (`±8 m GPS`), freshness (`2s ago`) and coordinates with a Copy button. Hide speed when it is null.

## Elevation & Depth

Two surface types:
- **Glass**, only on the status island, avatar stack and action dock: `rgb(16 18 21 / .6)`, `backdrop-filter: blur(20px) saturate(140%)`, a `0 0 0 1px rgb(255 255 255 / .10)` hairline ring and a deep soft shadow. Solid `--surface` fallback when blur is unsupported.
- **Solid**, for everything large (expanded list, bottom sheet, modals, toasts): `--surface` at about 92% opacity, 1 px `--border`, one layered shadow.

Never blur a large area. A 3 to 4% noise layer covers the app chrome, not the map.

## Shapes

Radius: controls 10 px, panels 16 px, pills and avatars full. Nested radius = outer minus padding.

## Components

Built from shadcn/ui (Radix), restyled to these tokens (never ship the default look). Bottom sheet `vaul`, palette `cmdk`, toasts `sonner`, icons `lucide-react` (one set, 1.5 px stroke), animation `motion`.

Markers: peer = 32 px circle with initials and a hue ring; you = 36 px accent fill with a dual-ring radar ripple, an accuracy circle and a heading cone (hidden when heading is null, rotates by the shortest angle); stale (over 25 s) = grayscale at 40% opacity with an "Idle" chip; clusters when zoomed out. Basemap: CARTO Dark Matter (dark) / Voyager (light) with slightly reduced saturation. The stored-history trail draws itself once per toggle (`stroke-dashoffset`), then settles to dashed.

## Motion

Durations 80 / 150 / 250 / 400 ms. Easings: enter `cubic-bezier(.16,1,.3,1)`, exit `cubic-bezier(.7,0,.84,0)`, move `cubic-bezier(.65,0,.35,1)`. Stagger 60 ms, capped at about 600 ms total. Animate only `transform`, `opacity`, `filter`. HUD panels use a spring `{ stiffness: 380, damping: 30 }`. Markers glide with `requestAnimationFrame` (never a CSS transition on the Leaflet marker element). Honour `prefers-reduced-motion`.

## Do's and Don'ts

**Do**
- Keep one accent visible per screen.
- Use real, specific copy.
- Design every state: empty, denied, offline, expired, loading.
- Keep text contrast at 4.5:1 or better and show a focus ring on every control.
- Show status with words as well as colour.

**Don't**
- Don't put glass on large panels, and don't use gradient text, purple gradients or emoji icons.
- Don't let the follow-me camera fight a manual pan or zoom.
- Don't promise "share link" anywhere: the app has no invite links.
- Don't use default OSM tiles.
- Don't give peers the accent colour.
- Don't animate `width`, `height`, `top` or `left`.
- Don't use `--text-faint` for text people need to read.
