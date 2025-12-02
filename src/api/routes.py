from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from src.ingestion.storage import DataStorage
from src.utils.logger import get_logger

router = APIRouter()
logger = get_logger(__name__)

@router.get("/food-gaps")
async def get_food_gaps():
    """
    Get food supply gaps by NTA as GeoJSON.
    """
    storage = DataStorage()
    try:
        engine = storage.get_engine()
        
        # Query to join NTA geometry with food gap data
        # We use ST_AsGeoJSON to get the geometry as a JSON string
        # We use json_build_object to construct the Feature properties
        # We use json_build_object again to construct the Feature
        # Finally we aggregate into a FeatureCollection
        query = text("""
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(
                    json_build_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(n.geom)::json,
                        'properties', json_build_object(
                            'nta_code', n.nta2020,
                            'nta_name', n.nta_name,
                            'boro_name', n.boro_name,
                            'year', f.year,
                            'supply_gap_lbs', f.supply_gap_lbs,
                            'food_insecure_pct', f.food_insecure_pct,
                            'vulnerable_pop_score', f.vulnerable_pop_score,
                            'unemployment_rate', f.unemployment_rate
                        )
                    )
                )
            ) as geojson
            FROM ntas_2020 n
            LEFT JOIN food_supply_gaps f ON n.nta2020 = f.nta_code
            WHERE f.year = (SELECT MAX(year) FROM food_supply_gaps) -- Get latest data
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
            
        return result
        
    except Exception as e:
        logger.error(f"Error fetching food gaps: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        storage.close()

@router.get("/poverty-by-zip")
async def get_poverty_by_zip():
    """Get poverty rates joined with ZCTA geometries."""
    # Note: We use ST_AsGeoJSON(geometry) because the column name in census_zctas_2020 is 'geometry'
    query = text("""
    SELECT 
        json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(
                json_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(z.geometry)::json,
                    'properties', json_build_object(
                        'zip_code', z.zip_code,
                        'year', c.year,
                        'poverty_rate', c.poverty_rate,
                        'median_household_income', c.median_household_income,
                        'poverty_count', c.poverty_count,
                        'poverty_universe', c.poverty_universe
                    )
                )
            )
        ) as geojson
    FROM census_zctas_2020 z
    JOIN census_acs_income_poverty c ON z.zip_code = c.zip_code
    WHERE c.year = (SELECT MAX(year) FROM census_acs_income_poverty)
      AND c.poverty_rate IS NOT NULL
      AND c.median_household_income IS NOT NULL;
    """)
    
    storage = DataStorage()
    try:
        engine = storage.get_engine()
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
        
        # If result is None (no data), return empty FeatureCollection
        if not result:
            return {"type": "FeatureCollection", "features": []}
            
        return result
    except Exception as e:
        logger.error(f"Error fetching poverty data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        storage.close()
