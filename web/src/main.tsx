import React from 'react';
import ReactDOM from 'react-dom/client';
import App from './App';
import './index.css';

import 'leaflet/dist/leaflet.css';
import 'leaflet-draw/dist/leaflet.draw.css';

import './lib/leafletGlobal';
import 'leaflet-draw';

import './lib/leafletIcons';

try {
  ReactDOM.createRoot(document.getElementById('root')!).render(
    <App />,
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
