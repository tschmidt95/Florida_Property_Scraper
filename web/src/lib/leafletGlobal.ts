import L from 'leaflet';

// Leaflet-draw expects a global `L` in many bundlers.
// This module should be imported once at startup (before importing leaflet-draw).
if (typeof window !== 'undefined') {
  (window as any).L = L;
}

export default L;
