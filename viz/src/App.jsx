import React, { useRef, useState, useEffect } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import ScrollyTelling from './components/ScrollyTelling';
import ComparisonTool from './components/ComparisonTool';

const MAPBOX_TOKEN = import.meta.env.VITE_MAPBOX_TOKEN;
const INITIAL_CENTER = [-74.006, 40.7128];
const INITIAL_ZOOM = 11;

// Define Story Chapters
const STORY_CHAPTERS = [
  {
    id: 'tribeca',
    title: 'The Starting Line: Tribeca',
    content: `
      Our journey begins in Tribeca (10007), one of the wealthiest zip codes in the country.
      Here, the median household income is astronomical,
      and market rents reflect a luxury lifestyle out of reach for most.
    `,
    location: { center: [-74.009, 40.715], zoom: 14.5, pitch: 45, bearing: 0 },
    zip: '10007',
    layer: 'rent'
  },
  {
    id: 'east-village',
    title: 'The Shift: East Village',
    content: `
      Moving north and east to the East Village (10009).
      Historically a haven for artists and immigrants, it has seen rapid gentrification.
      Rents are high, but the income gap is widening as students and professionals displace long-time residents.
    `,
    location: { center: [-73.982, 40.726], zoom: 14.5, pitch: 45, bearing: -15 },
    zip: '10009',
    layer: 'rent'
  },
  {
    id: 'east-harlem',
    title: 'The Divide: East Harlem',
    content: `
      Crossing 96th Street into East Harlem (10029), also known as El Barrio.
      The landscape changes dramatically. This is a vibrant community,
      but one facing significant challenges with high poverty rates and a severe rent burden.
    `,
    location: { center: [-73.945, 40.792], zoom: 14.5, pitch: 45, bearing: -20 },
    zip: '10029',
    layer: 'poverty'
  },
  {
    id: 'mott-haven',
    title: 'The Destination: Mott Haven',
    content: `
      Our final stop is Mott Haven (10454) in the South Bronx.
      Just a few miles from Tribeca, it feels like a different world.
      It has one of the lowest median incomes in the city.
      The "affordability" of rent here is an illusion when compared to local wages.
    `,
    location: { center: [-73.918, 40.806], zoom: 14.5, pitch: 45, bearing: -30 },
    zip: '10454',
    layer: 'poverty'
  }
];

function App() {
  const mapContainerRef = useRef(null);
  const mapRef = useRef(null);
  const [foodGapsData, setFoodGapsData] = useState(null);
  const [povertyData, setPovertyData] = useState(null);
  const [rentData, setRentData] = useState(null);
  const [hoverInfo, setHoverInfo] = useState(null);
  const [activeLayer, setActiveLayer] = useState('poverty'); // 'food-gap', 'poverty', or 'rent'

  // Story Mode State
  const [isStoryMode, setIsStoryMode] = useState(false);
  const [currentChapterIndex, setCurrentChapterIndex] = useState(0);
  const [storyData, setStoryData] = useState(null);

  // Comparison Tool State
  const [isComparisonMode, setIsComparisonMode] = useState(false);

  // Fetch data
  useEffect(() => {
    // Fetch Food Gap Data
    fetch('http://localhost:8000/api/food-gaps')
      .then(res => res.json())
      .then(data => setFoodGapsData(data))
      .catch(err => console.error('Error fetching food gaps:', err));

    // Fetch Poverty Data
    fetch('http://localhost:8000/api/poverty-by-zip')
      .then(res => res.json())
      .then(data => setPovertyData(data))
      .catch(err => console.error('Error fetching poverty data:', err));

    // Fetch Rent Data
    fetch('http://localhost:8000/api/rent-by-zip')
      .then(res => res.json())
      .then(data => setRentData(data))
      .catch(err => console.error('Error fetching rent data:', err));
  }, []);

  // Handle Story Chapter Change
  useEffect(() => {
    if (!isStoryMode || !mapRef.current || !povertyData || !rentData) return;

    const chapter = STORY_CHAPTERS[currentChapterIndex];

    // Fly to location
    mapRef.current.flyTo({
      ...chapter.location,
      duration: 2000,
      essential: true
    });

    // Extract data for the dashboard
    const povertyFeature = povertyData.features.find(f => f.properties.zip_code === chapter.zip);
    const rentFeature = rentData.features.find(f => f.properties.zip_code === chapter.zip);

    if (povertyFeature && rentFeature) {
      setStoryData({
        zip_code: chapter.zip,
        neighborhood_name: chapter.title.split(': ')[1],
        median_household_income: povertyFeature.properties.median_household_income,
        poverty_rate: povertyFeature.properties.poverty_rate,
        rent_index: rentFeature.properties.rent_index
      });
    }
  }, [currentChapterIndex, isStoryMode, povertyData, rentData]);

  // Initialize map
  useEffect(() => {
    if (!MAPBOX_TOKEN) return;
    if (!mapContainerRef.current) return;
    if (mapRef.current) return;

    mapboxgl.accessToken = MAPBOX_TOKEN;

    mapRef.current = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: 'mapbox://styles/mapbox/light-v11',
      center: INITIAL_CENTER,
      zoom: INITIAL_ZOOM
    });

    mapRef.current.addControl(new mapboxgl.NavigationControl(), 'top-right');

    mapRef.current.on('load', () => {
      // --- Food Gap Source & Layers ---
      mapRef.current.addSource('foodSupplyGaps', {
        type: 'geojson',
        data: foodGapsData || { type: 'FeatureCollection', features: [] }
      });

      mapRef.current.addLayer({
        id: 'nta-fills',
        type: 'fill',
        source: 'foodSupplyGaps',
        layout: {
          'visibility': activeLayer === 'food-gap' ? 'visible' : 'none'
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
          'visibility': activeLayer === 'food-gap' ? 'visible' : 'none'
        },
        paint: {
          'line-color': '#000000',
          'line-width': 1
        }
      });

      // --- Poverty Source & Layers ---
      mapRef.current.addSource('povertyData', {
        type: 'geojson',
        data: povertyData || { type: 'FeatureCollection', features: [] }
      });

      mapRef.current.addLayer({
        id: 'poverty-fills',
        type: 'fill',
        source: 'povertyData',
        layout: { visibility: activeLayer === 'poverty' ? 'visible' : 'none' },
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
        layout: { visibility: activeLayer === 'poverty' ? 'visible' : 'none' },
        paint: {
          'line-color': '#000000',
          'line-width': 1
        }
      });

      // --- Rent Source & Layers ---
      mapRef.current.addSource('rentData', {
        type: 'geojson',
        data: rentData || { type: 'FeatureCollection', features: [] }
      });

      mapRef.current.addLayer({
        id: 'rent-fills',
        type: 'fill',
        source: 'rentData',
        layout: { visibility: activeLayer === 'rent' ? 'visible' : 'none' },
        paint: {
          'fill-color': [
            'interpolate',
            ['linear'],
            ['get', 'rent_index'],
            1500, '#2cba00',
            2500, '#a3ff00',
            3500, '#fff400',
            4500, '#ffa700',
            5500, '#ff0000',
            6500, '#8b0000'
          ],
          'fill-opacity': 0.7
        }
      });

      mapRef.current.addLayer({
        id: 'rent-borders',
        type: 'line',
        source: 'rentData',
        layout: { visibility: activeLayer === 'rent' ? 'visible' : 'none' },
        paint: {
          'line-color': '#000000',
          'line-width': 1
        }
      });

      // --- Interactions ---
      const layers = ['nta-fills', 'poverty-fills', 'rent-fills'];

      layers.forEach(layer => {
        mapRef.current.on('mousemove', layer, (e) => {
          if (e.features.length > 0) {
            mapRef.current.getCanvas().style.cursor = 'pointer';
            setHoverInfo({
              feature: e.features[0],
              x: e.point.x,
              y: e.point.y
            });
          }
        });

        mapRef.current.on('mouseleave', layer, () => {
          mapRef.current.getCanvas().style.cursor = '';
          setHoverInfo(null);
        });
      });
    });

    return () => {
      mapRef.current.remove();
      mapRef.current = null;
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

    const rentSource = mapRef.current.getSource('rentData');
    if (rentSource && rentData) {
      rentSource.setData(rentData);
    }
  }, [foodGapsData, povertyData, rentData]);

  // Get current data year/date
  const getDataLabel = () => {
    if (activeLayer === 'food-gap' && foodGapsData?.features?.length > 0) {
      return `Data: ${foodGapsData.features[0].properties.year}`;
    }
    if (activeLayer === 'poverty' && povertyData?.features?.length > 0) {
      return `Data: ${povertyData.features[0].properties.year}`;
    }
    if (activeLayer === 'rent' && rentData?.features?.length > 0) {
      const dateStr = rentData.features[0].properties.date;
      if (dateStr) {
        const date = new Date(dateStr);
        return `Data: ${date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })}`;
      }
      return `Data: ${rentData.features[0].properties.year}`;
    }
    return null;
  };

  const dataLabel = getDataLabel();

  // Get source label
  const getSourceLabel = () => {
    if (activeLayer === 'food-gap') return 'Source: NYC Open Data';
    if (activeLayer === 'poverty') return 'Source: US Census ACS';
    if (activeLayer === 'rent') return 'Source: Zillow';
    return null;
  };

  const sourceLabel = getSourceLabel();

  // Toggle visibility
  useEffect(() => {
    if (!mapRef.current || !mapRef.current.isStyleLoaded()) return;

    let targetLayer = activeLayer;

    if (isStoryMode) {
      targetLayer = STORY_CHAPTERS[currentChapterIndex].layer;
    }

    const visibilityFood = targetLayer === 'food-gap' ? 'visible' : 'none';
    const visibilityPoverty = targetLayer === 'poverty' ? 'visible' : 'none';
    const visibilityRent = targetLayer === 'rent' ? 'visible' : 'none';

    if (mapRef.current.getLayer('nta-fills')) {
      mapRef.current.setLayoutProperty('nta-fills', 'visibility', visibilityFood);
      mapRef.current.setLayoutProperty('nta-borders', 'visibility', visibilityFood);
    }

    if (mapRef.current.getLayer('poverty-fills')) {
      mapRef.current.setLayoutProperty('poverty-fills', 'visibility', visibilityPoverty);
      mapRef.current.setLayoutProperty('poverty-borders', 'visibility', visibilityPoverty);
    }

    if (mapRef.current.getLayer('rent-fills')) {
      mapRef.current.setLayoutProperty('rent-fills', 'visibility', visibilityRent);
      mapRef.current.setLayoutProperty('rent-borders', 'visibility', visibilityRent);
    }
  }, [activeLayer, isStoryMode, currentChapterIndex]);

  if (!MAPBOX_TOKEN) {
    return <div style={{ padding: 20 }}>Please set VITE_MAPBOX_TOKEN in your .env file</div>;
  }

  return (
    <div style={{ position: 'relative', width: '100vw', height: '100vh', overflow: isStoryMode ? 'auto' : 'hidden' }}>
      <div ref={mapContainerRef} style={{ width: '100%', height: '100vh', position: isStoryMode ? 'fixed' : 'absolute', top: 0, left: 0 }} />

      {/* Story Mode Overlay */}
      {isStoryMode ? (
        <ScrollyTelling
          chapters={STORY_CHAPTERS}
          onChapterChange={setCurrentChapterIndex}
          currentData={storyData}
          onExit={() => {
            setIsStoryMode(false);
            // Reset map view
            mapRef.current.flyTo({ center: [-74.006, 40.7128], zoom: 11, pitch: 0, bearing: 0 });
          }}
        />
      ) : (
        <>
          {/* Normal Mode UI */}
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
                value="poverty"
                checked={activeLayer === 'poverty'}
                onChange={() => setActiveLayer('poverty')}
              /> Poverty Rate (by Zip)
            </label>
            <label style={{ display: 'block', marginBottom: 5 }}>
              <input
                type="radio"
                name="layer"
                value="food-gap"
                checked={activeLayer === 'food-gap'}
                onChange={() => setActiveLayer('food-gap')}
              /> Food Insecurity Gap (by NTA)
            </label>
            <label style={{ display: 'block', marginBottom: 10 }}>
              <input
                type="radio"
                name="layer"
                value="rent"
                checked={activeLayer === 'rent'}
                onChange={() => setActiveLayer('rent')}
              /> Market Rent (by Zip)
            </label>

            <button
              onClick={() => setIsStoryMode(true)}
              style={{
                width: '100%',
                padding: '8px',
                background: '#2c3e50',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                fontWeight: 'bold',
                marginBottom: '10px'
              }}
            >
              Start Story Mode 📖
            </button>

            <button
              onClick={() => setIsComparisonMode(true)}
              style={{
                width: '100%',
                padding: '8px',
                background: '#3498db',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                fontWeight: 'bold'
              }}
            >
              Compare Yourself 📊
            </button>
          </div>

          {isComparisonMode && (
            <ComparisonTool
              povertyData={povertyData}
              rentData={rentData}
              onExit={() => setIsComparisonMode(false)}
              onVisualize={(zipCode) => {
                const feature = povertyData?.features.find(f => f.properties.zip_code === zipCode);
                if (feature) {
                  const bounds = new mapboxgl.LngLatBounds();

                  const processCoords = (coords) => {
                    if (typeof coords[0] === 'number') {
                      bounds.extend(coords);
                    } else {
                      coords.forEach(processCoords);
                    }
                  };

                  if (feature.geometry.type === 'Polygon') {
                    feature.geometry.coordinates.forEach(ring => ring.forEach(coord => bounds.extend(coord)));
                  } else if (feature.geometry.type === 'MultiPolygon') {
                    feature.geometry.coordinates.forEach(poly => poly.forEach(ring => ring.forEach(coord => bounds.extend(coord))));
                  }

                  if (!bounds.isEmpty()) {
                    mapRef.current.fitBounds(bounds, { padding: 50, duration: 2000 });
                  }
                }
              }}
            />
          )}

          {hoverInfo && (
            <div className="tooltip" style={{ left: hoverInfo.x, top: hoverInfo.y }}>
              {activeLayer === 'food-gap' ? (
                <>
                  <div><strong>{hoverInfo.feature.properties.nta_name}</strong></div>
                  <div>Insecurity: {(hoverInfo.feature.properties.food_insecure_pct * 100).toFixed(1)}%</div>
                  <div>Gap: {Math.round(hoverInfo.feature.properties.supply_gap_lbs).toLocaleString()} lbs</div>
                </>
              ) : activeLayer === 'poverty' ? (
                <>
                  <div><strong>Zip: {hoverInfo.feature.properties.zip_code}</strong></div>
                  <div>Poverty Rate: {hoverInfo.feature.properties.poverty_rate}%</div>
                  <div>Median Income: ${parseInt(hoverInfo.feature.properties.median_household_income).toLocaleString()}</div>
                </>
              ) : (
                <>
                  <div><strong>Zip: {hoverInfo.feature.properties.zip_code}</strong></div>
                  <div>Market Rent: ${Math.round(hoverInfo.feature.properties.rent_index).toLocaleString()}</div>
                </>
              )}
            </div>
          )}

          <div className="legend">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 5 }}>
              <h3 style={{ margin: 0 }}>
                {activeLayer === 'food-gap' ? 'Food Insecurity %' :
                  activeLayer === 'poverty' ? 'Poverty Rate %' : 'Market Rent'}
              </h3>
            </div>
            <div style={{ fontSize: '0.8em', color: '#666', marginBottom: 5 }}>
              {sourceLabel && <div>{sourceLabel}</div>}
              {dataLabel && <div>{dataLabel}</div>}
            </div>
            <div className={`legend-gradient ${activeLayer}`}></div>
            <div className="legend-labels">
              {activeLayer === 'rent' ? (
                <>
                  <span>$1.5k</span>
                  <span>$6.5k+</span>
                </>
              ) : (
                <>
                  <span>0%</span>
                  <span>40%+</span>
                </>
              )}
            </div>
          </div>
        </>
      )}

      <style>{`
        .legend-gradient.poverty {
          background: linear-gradient(to right, #2cba00, #a3ff00, #fff400, #ffa700, #ff0000, #8b0000);
        }
        .legend-gradient.rent {
          background: linear-gradient(to right, #2cba00, #a3ff00, #fff400, #ffa700, #ff0000, #8b0000);
        }
      `}</style>
    </div>
  );
}

export default App;
