

	
	BEGIN

	CREATE TABLE IF NOT EXISTS datasci.stlk_geocode_staging (
		sate VARCHAR
		, miss_count INTEGER);

	TRUNCATE TABLE datasci.stlk_ggeocode_staging;

	SELECT INTO datasci.stlk_ggeocode_staging state
		, ROW_NUMBER() row_num
		, SUM(1) AS miss_count 
	FROM datasci.property_events pe 
	WHERE postgis_rating IS NULL 
	GROUP BY state 
	ORDER BY state

	END

	BEGIN

	DECLARE
		counter INTEGER := 1

		state VARCHAR := SELECT(state FROM datasci.stlk_ggeocode_staging
								row_num = counter)
		state_n INTEGER := SELECT(miss_count FROM datasci.stlk_ggeocode_staging
								row_num = counter)
		batchsize INTEGER := 0 

	WHILE counter < (SELECT SUM(1) FROM datasci.stlk_ggeocode_staging)) LOOP

		geocode_property_events(state_n, batchsize INTEGER, noticesize INTEGER, state)
