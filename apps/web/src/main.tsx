import { DEFAULT_TIMINGS } from '@llt/contracts';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

function App() {
  return (
    <main style={{ padding: 24 }}>
      <h1>Signal · Live Location Tracker</h1>
      <p>
        Phase 0 workspace shell. Full map UI (DESIGN.md + docs/plan/08-frontend.md) is built in
        Phase 3. Heartbeat: {DEFAULT_TIMINGS.heartbeatMs / 1000}s.
      </p>
    </main>
  );
}

const rootEl = document.getElementById('root');
if (rootEl) {
  createRoot(rootEl).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
}
