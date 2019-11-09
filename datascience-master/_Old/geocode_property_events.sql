CREATE OR REPLACE FUNCTION geocode_property_events(maxn INTEGER, batchsize INTEGER) RETURNS VOID AS
$$
DECLARE
   counter INTEGER := 0 ; 
   n INTEGER := (SELECT count(postgis_rating) 
		FROM datasci.property_events 
		WHERE datasci.property_events IS NULL) ; 
BEGIN
	
	IF (maxn < n) THEN 
		n := maxn;
	END IF;

	WHILE counter <= n LOOP
		
		counter := counter + batchsize;
		
		UPDATE datasci.property_events
			SET postgis_rating = g2.postgis_rating
    			, lon = g2.lon
        		, lat = g2.lat
		FROM (
			SELECT pe.id
				, COALESCE((g.geo).rating,-1) as postgis_rating
				, ST_X((g.geo).geomout) as lon
    			, ST_Y((g.geo).geomout) as lat
			FROM (
				SELECT (geocode(address,1)) As geo
    			FROM datasci.property_events As pe
				WHERE pe.postgis_rating IS NULL   
    			ORDER BY pe.id  	
    			LIMIT batchsize) g
    		ORDER BY pe.id  	
    		) g2; 	
		
	END LOOP;

END

$$  LANGUAGE plpgsql