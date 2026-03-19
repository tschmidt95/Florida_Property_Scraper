import React from 'react';
import ReactDOM from 'react-dom/client';
import MapSearch from './pages/MapSearch';
import './index.css';

import 'leaflet/dist/leaflet.css';
import 'leaflet-draw/dist/leaflet.draw.css';

import './lib/leafletGlobal';
import 'leaflet-draw';

import './lib/leafletIcons';

class RootErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { hasError: boolean; message: string }
> {
  state = { hasError: false, message: '' };

  static getDerivedStateFromError(err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return { hasError: true, message };
  }

  componentDidCatch(error: unknown) {
    // eslint-disable-next-line no-console
    console.error('Runtime root error', error);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ fontFamily: 'ui-sans-serif, system-ui', padding: 16 }}>
          <div style={{ fontWeight: 700 }}>Frontend runtime error</div>
          <div style={{ marginTop: 8, color: '#555' }}>{this.state.message}</div>
          <div style={{ marginTop: 8, color: '#555' }}>MapSearch failed to render. Check browser console.</div>
        </div>
      );
    }
    return this.props.children;
  }
}

try {
  ReactDOM.createRoot(document.getElementById('root')!).render(
    <RootErrorBoundary>
      <MapSearch />
    </RootErrorBoundary>,
  );
} catch (e) {
  // If React crashes before rendering, ensure we still show something.
  const msg = e instanceof Error ? e.message : String(e);
  // eslint-disable-next-line no-console
  console.error('Fatal frontend error', e);
  document.body.innerHTML = `
    <div style="font-family: ui-sans-serif, system-ui; padding: 16px;">
      <div style="font-weight: 700;">Frontend failed to start</div>
      <div style="margin-top: 8px; color: #555;">${msg}</div>
      <div style="margin-top: 8px; color: #555;">Check console + web/BUILD_ERRORS.txt</div>
    </div>
  `;
}
