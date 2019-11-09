select id as  create_id,
	"type",
	acting_user_id,
	object_id,
	created_at as create_created_at,
	updated_at timestamptz NULL,
	event_data text NULL,
	object_type text NULL,
	street_addr text NULL,
	city text NULL,
	state text NULL,
	zipcode text NULL,
	property_transaction_id text NULL,
	address text NULL,
	lat float8 NULL,
	lon float8 NULL,
	external_account_id text NULL,
	originating_message_id text NULL,

from datasci.property_transactions_ds
where type IN ('PropertyTransactionAutomaticallyCreatedEvent'
				,'PropertyTransactionManuallyCreatedEvent') ain
left join (select *
			from datasci.property_transactions_ds
			where type IN ('PropertyTransactionConfirmedEvent'
				,'PropertyTransactionRejectedEvent')) aout
	on ain.property_transaction_id = aout.property_transaction_id
left join (select *
			from datasci.property_transactions_ds
			where type IN ('PropertyTransactionDeletedEvent')) adel
	on ain.property_transaction_id = aout.property_transaction_id




	CREATE OR REPLACE VIEW datasci_projects.vw_offer_extraction AS
 SELECT dsom.id,
    dsom.event_id,
    dsom.property_transaction_id,
    dsom.event_type,
    dsom.representation,
    dspt.acting_user_id,
    dspt.external_account_id,
    dspt.created_at,
    dsom.message_id,
    dsom."from",
    dsom."to",
    dsom.subject,
    dsom.date,
    dsom.body_text,
    dsod.attachment_name,
    dsod.document_content,
    dsod.parsed_document_data,
    dspt.street_addr,
    dspt.city,
    dspt.state,
    dspt.zipcode,
    dspt.lat,
    dspt.lon,
    row_number() OVER (PARTITION BY dspt.street_addr, dspt.city, dspt.state ORDER BY (dspt.created_at - dsom.date::timestamp with time zone)) AS rown,
    date_part('day'::text, dspt.created_at - dsom.date::timestamp with time zone) AS daysfromtxcreate,
    md5(dsod.document_content) AS md5
   FROM datasci.offer_messages dsom
     LEFT JOIN datasci.offer_documents dsod ON dsom.message_id::text = dsod.message_id::text AND dsom.property_transaction_id = dsod.property_transaction_id
     LEFT JOIN datasci.property_transactions_ds dspt ON dspt.id = dsom.event_id
  WHERE dsom.missing_reason IS NULL AND dsod.id IS NOT NULL; 
  --AND dsod.document_content ~~ '%RPA-CA%'::text;



CREATE OR REPLACE VIEW datasci_projects.vw_purchase_offers AS
 SELECT pea.id::text AS agent_id,
    md5(dsom."from"::text) AS offer_message_from,
    md5(dsom."to"::text) AS offer_message_to,
    dsom.representation AS agent_representation,
    dspt.property_transaction_id,
    dspt.address,
    dspt.state,
    dspt.lat,
    dspt.lon,
    dspt.tract_id AS census_tract,
    md5(dsppo.offer_listing_agent) AS offer_listing_agent,
    md5(dsppo.offer_buying_agent) AS offer_buying_agent,
    md5(dsppo.buyer_name) AS buyer_name,
    dsppo.offer_date,
    dsppo.offer_price,
    dsppo.cash_sale_amt,
    dsppo.financed_sale_amt,
    dsppo.seller_paid_title_insurance,
    dsppo.buyer_paid_title_insurance,
    dsppo.title_issuer,
    dsppo.option_fee,
    dsppo.option_days,
    row_number() OVER (PARTITION BY dspt.address, dspt.property_transaction_id ORDER BY dsppo.offer_date) AS prop_txo_num,
    count(*) OVER (PARTITION BY dspt.address, dspt.property_transaction_id) AS offer_count
   FROM ( SELECT purchase_offers_tx.om_id,
            purchase_offers_tx.seller_name,
            purchase_offers_tx.buyer_name,
            purchase_offers_tx.exclusions,
            purchase_offers_tx.cash_sale_amt,
            purchase_offers_tx.financed_sale_amt,
            purchase_offers_tx.offer_price,
            purchase_offers_tx.earnest_money,
            purchase_offers_tx.escrow_agent,
            purchase_offers_tx.seller_paid_title_insurance,
            purchase_offers_tx.buyer_paid_title_insurance,
            purchase_offers_tx.title_issuer,
            purchase_offers_tx.closing_date,
            purchase_offers_tx.option_fee,
            purchase_offers_tx.option_days,
            purchase_offers_tx.offer_date::date AS offer_date,
            NULL::text AS offer_listing_agent,
            NULL::text AS offer_buying_agent
           FROM purchase_offers_tx
        UNION
         SELECT purchase_offers.id AS om_id,
            NULL::text AS seller_name,
            purchase_offers.offer_buyer AS buyer_name,
            NULL::text AS exclusions,
            NULL::text AS cash_sale_amt,
            NULL::text AS financed_sale_amt,
            purchase_offers.offer_price,
            NULL::text AS earnest_money,
            NULL::text AS escrow_agent,
            NULL::text AS seller_paid_title_insurance,
            NULL::text AS buyer_paid_title_insurance,
            NULL::text AS title_issuer,
            NULL::text AS closing_date,
            NULL::text AS option_fee,
            NULL::text AS option_days,
            purchase_offers.offer_date::date AS offer_date,
            purchase_offers.offer_listing_agent,
            purchase_offers.offer_buying_agent
           FROM purchase_offers) dsppo
     LEFT JOIN datasci_minig.offer_messages dsom ON dsppo.om_id = dsom.id
     LEFT JOIN datasci.property_transactions_ds dspt ON dsom.event_id = dspt.id
     LEFT JOIN public.external_accounts pea ON dspt.external_account_id::integer = pea.id
  WHERE dspt.state = ANY (ARRAY['TX'::text, 'CA'::text])
  ORDER BY dspt.address, dspt.property_transaction_id, dsppo.offer_date;    
