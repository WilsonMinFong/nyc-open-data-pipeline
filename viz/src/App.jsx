import React, { useRef, useState, useEffect } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';

const MAPBOX_TOKEN = import.meta.env.VITE_MAPBOX_TOKEN;
const INITIAL_CENTER = [-74.006, 40.7128];
const INITIAL_ZOOM = 10;

function App() {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const [foodGapsData, setFoodGapsData] = useState(null);
  const [povertyData, setPovertyData] = useState(null);
  const [activeLayer, setActiveLayer] = useState('food-gap'); // 'food-gap' or 'poverty'
  const [hoverInfo, setHoverInfo] = useState(null);

  // Fetch data
  useEffect(() => {
    // Fetch Food Gaps
    fetch('http://localhost:8000/api/food-gaps')
      .then(resp => resp.json())
      .then(json => setFoodGapsData(json))
      .catch(err => console.error('Failed to load food gaps data', err));

    // Fetch Poverty Data
    fetch('http://localhost:8000/api/poverty-by-zip')
      .then(resp => resp.json())
      .then(json => setPovertyData(json))
      .catch(err => console.error('Failed to load poverty data', err));
  }, []);

  // Initialize map
  useEffect(() => {
    if (!MAPBOX_TOKEN) return;

    mapboxgl.accessToken = MAPBOX_TOKEN;

    mapRef.current = new mapboxgl.Map({
      container: mapContainerRef.current,
      center: INITIAL_CENTER,
      zoom: INITIAL_ZOOM,
    });

    mapRef.current.addControl(new mapboxgl.NavigationControl(), 'top-right');

    mapRef.current.on('load', () => {
      // Add sources
      mapRef.current.addSource('foodSupplyGaps', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] }
      });

      mapRef.current.addSource('povertyData', {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] }
      });

      // --- Food Gap Layers ---
      mapRef.current.addLayer({
        id: 'nta-fills',
        type: 'fill',
        source: 'foodSupplyGaps',
        layout: {
          'visibility': 'visible'
        },
        paint: {
          'fill-color': [
            'interpolate',
            ['linear'],
            ['get', 'food_insecure_pct'],
            0.0, '#2cba00',
            0.1, '#a3ff00',
            0.15, '#fff400',
            0.2, '#ffa700',
            0.3, '#ff0000',
            0.4, '#8b0000'
          ],
          'fill-opacity': 0.7
        }
      });

      mapRef.current.addLayer({
        id: 'nta-borders',
        type: 'line',
        source: 'foodSupplyGaps',
        layout: {
          'visibility': 'visible'
        },
        paint: {
          'line-color': '#000',
          'line-width': 0.5
        }
      });

      // --- Poverty Layers ---
      mapRef.current.addLayer({
        id: 'poverty-fills',
        type: 'fill',
        source: 'povertyData',
        layout: {
          'visibility': 'none'
        },
        paint: {
          'fill-color': [
            'interpolate',
            ['linear'],
            ['get', 'poverty_rate'],
            0, '#2cba00',
            10, '#a3ff00',
            15, '#fff400',
            20, '#ffa700',
            30, '#ff0000',
            40, '#8b0000'
          ],
          'fill-opacity': 0.7
        }
      });

      mapRef.current.addLayer({
        id: 'poverty-borders',
        type: 'line',
        source: 'povertyData',
        layout: {
          'visibility': 'none'
        },
        paint: {
          'line-color': '#000',
          'line-width': 0.5
        }
      });
    });

    // Hover effect
    const onMouseMove = (e) => {
      if (e.features.length > 0) {
        const feature = e.features[0];
        setHoverInfo({
          feature: feature,
          x: e.point.x,
          y: e.point.y
        });
        mapRef.current.getCanvas().style.cursor = 'pointer';
      }
    };

    const onMouseLeave = () => {
      setHoverInfo(null);
      mapRef.current.getCanvas().style.cursor = '';
    };

    mapRef.current.on('mousemove', 'nta-fills', onMouseMove);
    mapRef.current.on('mouseleave', 'nta-fills', onMouseLeave);
    mapRef.current.on('mousemove', 'poverty-fills', onMouseMove);
    mapRef.current.on('mouseleave', 'poverty-fills', onMouseLeave);

    return () => {
      mapRef.current.remove();
    };
  }, []);

  // Update data sources
  useEffect(() => {
    if (!mapRef.current || !mapRef.current.isStyleLoaded()) return;

    const foodSource = mapRef.current.getSource('foodSupplyGaps');
    if (foodSource && foodGapsData) {
      foodSource.setData(foodGapsData);
    }

    const povertySource = mapRef.current.getSource('povertyData');
    if (povertySource && povertyData) {
      povertySource.setData(povertyData);
    }
  }, [foodGapsData, povertyData]);

  // Get current data year
  const getDataYear = () => {
    if (activeLayer === 'food-gap' && foodGapsData?.features?.length > 0) {
      return foodGapsData.features[0].properties.year;
    }
    if (activeLayer === 'poverty' && povertyData?.features?.length > 0) {
      return povertyData.features[0].properties.year;
    }
    return null;
  };

  const dataYear = getDataYear();

  // Toggle visibility
  useEffect(() => {
    if (!mapRef.current || !mapRef.current.isStyleLoaded()) return;

    const visibilityFood = activeLayer === 'food-gap' ? 'visible' : 'none';
    const visibilityPoverty = activeLayer === 'poverty' ? 'visible' : 'none';

    if (mapRef.current.getLayer('nta-fills')) {
      mapRef.current.setLayoutProperty('nta-fills', 'visibility', visibilityFood);
      mapRef.current.setLayoutProperty('nta-borders', 'visibility', visibilityFood);
    }

    if (mapRef.current.getLayer('poverty-fills')) {
      mapRef.current.setLayoutProperty('poverty-fills', 'visibility', visibilityPoverty);
      mapRef.current.setLayoutProperty('poverty-borders', 'visibility', visibilityPoverty);
    }
  }, [activeLayer]);

  if (!MAPBOX_TOKEN) {
    return <div style={{ padding: 20 }}>Please set VITE_MAPBOX_TOKEN in your .env file</div>;
  }

  return (
    <div style={{ position: 'relative', width: '100vw', height: '100vh' }}>
      <div ref={mapContainerRef} style={{ width: '100%', height: '100%' }} />

      {/* Layer Toggle */}
      <div style={{
        position: 'absolute',
        top: 20,
        left: 20,
        background: 'white',
        padding: 10,
        borderRadius: 4,
        boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
        zIndex: 1
      }}>
        <div style={{ marginBottom: 5, fontWeight: 'bold' }}>Select Layer:</div>
        <label style={{ display: 'block', marginBottom: 5 }}>
          <input
            type="radio"
            name="layer"
            value="food-gap"
            checked={activeLayer === 'food-gap'}
            onChange={() => setActiveLayer('food-gap')}
          /> Food Insecurity Gap
        </label>
        <label style={{ display: 'block' }}>
          <input
            type="radio"
            name="layer"
            value="poverty"
            checked={activeLayer === 'poverty'}
            onChange={() => setActiveLayer('poverty')}
          /> Poverty Rate (by Zip)
        </label>
      </div>

      {hoverInfo && (
        <div className="tooltip" style={{ left: hoverInfo.x, top: hoverInfo.y }}>
          {activeLayer === 'food-gap' ? (
            <>
              <div><strong>{hoverInfo.feature.properties.nta_name}</strong></div>
              <div>Insecurity: {(hoverInfo.feature.properties.food_insecure_pct * 100).toFixed(1)}%</div>
              <div>Gap: {Math.round(hoverInfo.feature.properties.supply_gap_lbs).toLocaleString()} lbs</div>
            </>
          ) : (
            <>
              <div><strong>Zip: {hoverInfo.feature.properties.zip_code}</strong></div>
              <div>Poverty Rate: {hoverInfo.feature.properties.poverty_rate}%</div>
              <div>Median Income: ${parseInt(hoverInfo.feature.properties.median_household_income).toLocaleString()}</div>
            </>
          )}
        </div>
      )}

      <div className="legend">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
          <h3>{activeLayer === 'food-gap' ? 'Food Insecurity %' : 'Poverty Rate %'}</h3>
          {dataYear && <span style={{ fontSize: '0.8em', color: '#666' }}>Data: {dataYear}</span>}
        </div>
        <div className={`legend-gradient ${activeLayer}`}></div>
        <div className="legend-labels">
          <span>0%</span>
          <span>40%+</span>
        </div>
      </div>

      <style>{`
        .legend-gradient.poverty {
          background: linear-gradient(to right, #2cba00, #a3ff00, #fff400, #ffa700, #ff0000, #8b0000);
        }
      `}</style>
    </div>
  );
}

export default App;
