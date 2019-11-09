CREATE OR REPLACE FUNCTION geocode_property_events(maxn INTEGER, batchsize INTEGER, noticesize INTEGER) RETURNS VOID AS
$$
DECLARE
	
	counter INTEGER := 0 ; 
	n INTEGER := 0;
	g INTEGER := 0;
	n_remaining INTEGER := 0;

	notice_counter INTEGER := noticesize;

BEGIN
	
	n := (SELECT SUM(1)
		FROM datasci.property_events pe
		WHERE pe.postgis_rating IS NULL);
	g := (SELECT SUM(1)
		FROM datasci.property_events pe
		WHERE pe.postgis_rating IS NOT NULL);

	IF (batchsize > maxn) THEN 
		
        maxn := batchsize;
        RAISE NOTICE 'Batch size is greater than max records to code';
        RAISE NOTICE 'Setting max records to code to (%)', maxn;        
	
    END IF;

	IF (batchsize > noticesize) THEN 
		
        noticesize := batchsize;
        RAISE NOTICE 'Batch size is greater than notice size';
        RAISE NOTICE 'Setting notice size to (%)', noticesize;        
	
    END IF;

	IF (maxn > 0 AND maxn < n) THEN 
		
        n := maxn;
	
    END IF;
	
	n_remaining := n;

	RAISE NOTICE '(%) table records have geocode', g;
	RAISE NOTICE 'Attempting batch geocode of (%) records', n;
	RAISE NOTICE 'At a batch size of (%)', batchsize;

	WHILE counter <= n LOOP
		
		UPDATE datasci.property_events pe
			SET postgis_rating = g2.postgis_rating
    			, lon = g2.lon
        		, lat = g2.lat
		FROM (
			SELECT g.id
				, COALESCE((g.geo).rating,-1) as postgis_rating
				, ST_X((g.geo).geomout) as lon
    			, ST_Y((g.geo).geomout) as lat
			FROM (
				SELECT pe.id
					, (geocode(REPLACE(address,'''',''),1)) As geo
    			FROM datasci.property_events As pe
				WHERE pe.postgis_rating IS NULL   
    			ORDER BY pe.id  	
    			LIMIT batchsize) g
    		ORDER BY g.id) g2
        WHERE g2.id = pe.id
        AND pe.postgis_rating IS NULL;

	counter := counter + 1;	

	n_remaining := n - (counter * batchsize);

    IF ((n - n_remaining) >= notice_counter) THEN
    	
    	RAISE NOTICE '(%) Records Geocoded', n - n_remaining;				
		RAISE NOTICE '(%) Records Remaining', n_remaining;
    	notice_counter := notice_counter + noticesize;

    END IF;

	END LOOP;

END

$$  LANGUAGE plpgsql