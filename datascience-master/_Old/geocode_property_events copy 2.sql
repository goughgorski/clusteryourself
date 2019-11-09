CREATE OR REPLACE FUNCTION geocode_property_events(maxn INTEGER, batchsize INTEGER, noticesize INTEGER, state_eval VARCHAR) RETURNS VOID AS
$$
DECLARE
	
	counter INTEGER := 0 ; 
	n INTEGER := 0;
	g INTEGER := 0;
	n_remaining INTEGER := 0;

	notice_counter INTEGER := noticesize;

	start_time TIMESTAMP := CLOCK_TIMESTAMP();
	stop_time TIMESTAMP := CLOCK_TIMESTAMP();
	time_diff INTERVAL := start_time - stop_time;
	cum_time INTERVAL := time_diff;
    avg_time INTERVAL := time_diff/1;

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

	CREATE TABLE IF NOT EXISTS datasci.geocode_staging (
		id INTEGER
		, postgis_rating INTEGER
		, lon DECIMAL
		, lat DECIMAL);

	WHILE counter*batchsize < n LOOP
		
		start_time := CLOCK_TIMESTAMP();

		TRUNCATE TABLE datasci.geocode_staging;

		IF (batchsize > n_remaining) THEN

			batchsize := n_remaining;

			RAISE NOTICE 'Resetting batchsize to (%) to optimze table access', batchsize;

		END IF;

		INSERT INTO datasci.geocode_staging (id, postgis_rating, lon, lat)
		SELECT g2.id
			,g2.postgis_rating
            ,g2.lon
        	,g2.lat
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
				AND pe.state LIKE state_eval   	
    			ORDER BY pe.state, pe.zip
                LIMIT batchsize) g
    		) g2;

		UPDATE datasci.property_events pe
			SET postgis_rating = gs.postgis_rating
    			, lon = gs.lon
        		, lat = gs.lat
			FROM datasci.geocode_staging gs
		    WHERE gs.id = pe.id;

	counter := counter + 1;	

	n_remaining := n - (counter * batchsize);

    IF ((n - n_remaining) >= notice_counter) THEN
    	
    	RAISE NOTICE '(%) Records Geocoded', n - n_remaining;				
		RAISE NOTICE '(%) Records Remaining', n_remaining;
    	notice_counter := notice_counter + noticesize;

    END IF;
    
	stop_time := CLOCK_TIMESTAMP();
	time_diff := stop_time - start_time;
	cum_time := cum_time + time_diff;


	END LOOP;

	avg_time := cum_time/counter;
	RAISE NOTICE 'Process completed in: (%)', cum_time;
	RAISE NOTICE 'Averge geocoding time: (%)', avg_time;

END

$$  LANGUAGE plpgsql


