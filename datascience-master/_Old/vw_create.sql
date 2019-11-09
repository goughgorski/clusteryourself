
CREATE OR REPLACE VIEW datasci.vw_property_geocode_queue AS
 SELECT *
    , CASE WHEN dsp.gid IS NULL THEN true ELSE false end as needs_census_geocode
    , CASE WHEN (dsp.lat IS NULL OR dsp.lon IS NULL) THEN true else false end as needs_coordinate_geocode
 FROM datasci.properties dsp
 WHERE dsp.gid IS NULL 
 OR dsp.lat IS NULL 
 OR dsp.lon IS NULL;

CREATE OR REPLACE VIEW datasci.vw_property_transaction_events_queue AS 
  SELECT pe.*
  FROM public.events pe
  LEFT JOIN datasci.property_transaction_events dspte
    ON pe.id = dspte.event_id
  LEFT JOIN datasci.quarantined_property_transaction_events dsqpte
    on pe.id = dsqpte.event_id   
  WHERE 1=1
  AND dspte.id IS NULL 
  AND dsqpte.id IS NULL
  AND pe."type" IN  ('PropertyTransactionConfirmedEvent'
            ,'PropertyTransactionDeletedEvent'
            ,'PropertyTransactionManuallyCreatedEvent'
            ,'PropertyTransactionRejectedEvent'
            ,'PropertyTransactionAutomaticallyCreatedEvent'
            ,'PropertyTransactionFellThroughEvent');

CREATE OR REPLACE VIEW datasci.vw_property_transaction_timeline_events_queue AS 
  SELECT pe.*
  FROM public.events pe
  LEFT JOIN datasci.property_transaction_timeline_events dsptte
    on pe.id = dsptte.event_id
  LEFT JOIN datasci.quarantined_property_transaction_timeline_events dsqptte
    on pe.id = dsqptte.event_id 
  WHERE 1=1
  AND dsptte.id IS NULL
  AND dsqptte.id IS NULL
  AND pe."type" IN  ('TransactionTimelineEventCreatedEvent'
              ,'TransactionTimelineEventDestroyedEvent'
              ,'TransactionTimelineEventUpdatedEvent');


CREATE OR REPLACE VIEW datasci_mining.vw_purchase_offers AS
 SELECT pea.id::text AS agent_id,
    md5(dsom."from"::text) AS offer_message_from,
    md5(dsom."to"::text) AS offer_message_to,
    dsom.representation AS agent_representation,
    dspt.property_transaction_id,
    dsp.address,
    dsp.state,
    dsp.lat,
    dsp.lon,
    dsp.tract_id AS census_tract,
    seller_name,
    buyer_name,
    lot_number,
    block_number,
    addition,
    lot_or_parcel_number,
    exclusions,
    cash_sale_amt,
    financed_sale_amt,
    offer_date_str,
    offer_price, 
    broker_disclosure, 
    earnest_money,
    earnest_money_add,
    earnest_money_add_days,
    add_escrow_amt,
    all_cash_offer,
    escrow_agent,
    escrow_office_addr,
    title_insurance,
    title_issuer,
    closing_date,
    closing_date_acceptance_relative,
    option_fee,
    option_days,
    mandatory_hoa,
    add_fin_terms_str,
    executed_date,
    executed_date_str,
    executed_str,
    listing_to_buyer_broker_fee,
    buyer_agent,
    seller_agent,
    buyer_agent_lic,
    seller_agent_lic,
    buyer_agent_rep,
    seller_agent_rep,
    buyer_agent_rep_lic,
    seller_agent_rep_lic,
    buyer_agent_2,
    seller_agent_2,
    row_number() OVER (PARTITION BY dsp.address, dspt.property_transaction_id ORDER BY (dsmppo.closing_date::date)) AS prop_txo_num,
    COUNT(*) OVER (PARTITION BY dsp.address, dspt.property_transaction_id) AS offer_count
   FROM ( SELECT doc_id
            , seller_name
            , buyer_name
            , lot_number
            , block_number
            , addition
            , lot_or_parcel_number
            , exclusions
            , cash_sale_amt
            , financed_sale_amt
            , offer_date_str
            , offer_price
            , broker_disclosure
            , earnest_money
            , earnest_money_add
            , earnest_money_add_days
            , add_escrow_amt
            , all_cash_offer
            , escrow_agent
            , escrow_office_addr
            , title_insurance
            , title_issuer
            , closing_date
            , closing_date_acceptance_relative
            , option_fee
            , option_days
            , mandatory_hoa
            , add_fin_terms_str
            , executed_date
            , executed_date_str
            , executed_str
            , listing_to_buyer_broker_fee
            , buyer_agent
            , seller_agent
            , buyer_agent_lic
            , seller_agent_lic
            , buyer_agent_rep
            , seller_agent_rep
            , buyer_agent_rep_lic
            , seller_agent_rep_lic
            , buyer_agent_2
            , seller_agent_2
          FROM datasci_mining.parsed_purchase_offers
        ) dsmppo
     left join datasci_mining.offer_documents dsmod on dsmod.id = dsmppo.doc_id::integer
        LEFT JOIN datasci_mining.offer_messages dsom ON dsmod.message_id = dsom.message_id
     LEFT JOIN datasci.property_transactions dspt ON dsom.property_transaction_id = dspt.property_transaction_id
     LEFT JOIN datasci.properties dsp ON dspt.property_id = dsp.id
     LEFT JOIN public.external_accounts pea ON dspt.external_account_id::integer = pea.id
     INNER JOIN datasci_mining.document_parsers dsmdp ON dsp.state = dsmdp.state
  ORDER BY dsp.address, dspt.property_transaction_id, dsmppo.closing_date;