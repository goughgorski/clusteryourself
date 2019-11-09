CREATE OR REPLACE FUNCTION datasci.update_transaction_complexity() RETURNS VOID AS
$$

BEGIN 

	CREATE TABLE IF NOT EXISTS datasci.transaction_complexity_agg (	
			property_transaction_id INTEGER
    		, email_count INTEGER
    		, min_email_created TIMESTAMP WITHOUT TIME ZONE
    		, max_email_created TIMESTAMP WITHOUT TIME ZONE
    		, doc_count INTEGER
    		, min_doc_created TIMESTAMP WITHOUT TIME ZONE
    		, max_doc_created TIMESTAMP WITHOUT TIME ZONE
    		, contact_count INTEGER
    		, min_contact_created TIMESTAMP WITHOUT TIME ZONE
    		, max_contact_created TIMESTAMP WITHOUT TIME ZONE
			);

	CREATE TEMP TABLE transaction_complexity_agg_temp AS 
	SELECT vtca.*
	FROM datasci.vw_transaction_complexity_agg vtca
	EXCEPT
	SELECT *
	FROM datasci.transaction_complexity_agg tca; 

	UPDATE datasci.transaction_complexity_agg tca
		SET  property_transaction_id = tcat.property_transaction_id
			, email_count = tcat.email_count
			, min_email_created = tcat.min_email_created
			, max_email_created = tcat.max_email_created
			, doc_count = tcat.doc_count
			, min_doc_created = tcat.min_doc_created
			, max_doc_created = tcat.max_doc_created
			, contact_count = tcat.contact_count
			, min_contact_created = tcat.min_contact_created
			, max_contact_created = tcat.max_contact_created
		FROM transaction_complexity_agg_temp tcat
	   	WHERE tca.property_transaction_id = tcat.property_transaction_id
	   	;

	INSERT INTO datasci.transaction_complexity_agg 
	SELECT tcat.*
	FROM transaction_complexity_agg_temp tcat
	LEFT JOIN datasci.transaction_complexity_agg tca
		ON tca.property_transaction_id = tcat.property_transaction_id
	WHERE tca.property_transaction_id IS NULL
	;

END;

$$  LANGUAGE plpgsql



