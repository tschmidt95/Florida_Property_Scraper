import { useEffect } from 'react';

import { MapContainer, TileLayer } from 'react-leaflet';

type MapStatus = 'loading' | 'loaded' | 'failed';

export default function SafeMap({
  onStatus,
}: {
  onStatus?: (status: MapStatus) => void;
}) {
  useEffect(() => {
    onStatus?.('loading');
  }, [onStatus]);

  return (
    <div className="h-[70vh] min-h-[420px] w-full">
      <MapContainer
        center={[28.5383, -81.3792]}
        zoom={12}
        style={{ height: '100%', width: '100%' }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          eventHandlers={{
            load: () => onStatus?.('loaded'),
            tileerror: () => onStatus?.('failed'),
          }}
        />
      </MapContainer>
    </div>
  );
}
