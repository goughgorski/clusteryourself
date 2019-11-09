
CREATE OR REPLACE VIEW datasci_projects.vw_market_coverage_inputs AS
  SELECT lhs.property_transaction_id,
    CONCAT(lhs.street_addr, ' ', lhs.unit, ', ', lhs.city, ', ', lhs.state, ' ', lhs.zipcode) as address,
    lhs.street_addr,
    lhs.unit,
    lhs.city,
    lhs.state,
    lhs.zipcode,
    lhs.created_at,
    lhs.type,
    lhs.timeline_id,
    ptdel.deleted,
    ptcl.closing_date,
    lhs.external_account_id,
    lhs.token_created_at
  FROM ( 
    SELECT ppt.id AS property_transaction_id,
      pa.street_addr,
      pa.unit,        
      pa.city,
      pa.state,
      pa.zipcode,
      ppt.created_at,
      'PropertyTransactionAutomaticallyCreatedEvent'::text AS type,
      ppt.timeline_id,
      ppt.external_account_id,
      pea.created_at AS token_created_at
    FROM public.property_transactions ppt
    LEFT JOIN public.external_accounts pea ON ppt.external_account_id = pea.id
    LEFT JOIN public.addresses pa ON ppt.address_id = pa.id
    WHERE ppt.status = 0
    UNION
    SELECT dspt.property_transaction_id,
      dspt.street_addr,
      NULL as unit,         
      dsp.city,
      dsp.state,
      dsp.zipcode,         
      dspt.created_at,
      dspt.type,
      ppt.timeline_id,
      dspt.external_account_id::integer AS external_account_id,
      pea.created_at AS token_created_at
    FROM datasci.property_transaction_events dspt
    LEFT JOIN public.property_transactions ppt ON dspt.property_transaction_id = ppt.id
    LEFT JOIN public.external_accounts pea ON dspt.external_account_id::integer = pea.id
    LEFT JOIN datasci.properties dsp ON dspt.property_id = dsp.id    
    WHERE dspt.type = ANY (ARRAY['PropertyTransactionManuallyCreatedEvent'::text, 'PropertyTransactionConfirmedEvent'::text, 'PropertyTransactionRejectedEvent'::text])
  ) lhs
LEFT JOIN ( 
  SELECT property_transaction_events.property_transaction_id,
    property_transaction_events.type AS deleted
  FROM datasci.property_transaction_events
  WHERE property_transaction_events.type = 'PropertyTransactionDeletedEvent'::text
  ) ptdel 
  ON lhs.property_transaction_id = ptdel.property_transaction_id
LEFT JOIN ( 
  SELECT transaction_timeline_events.transaction_timeline_id,
  transaction_timeline_events.date AS closing_date
  FROM public.transaction_timeline_events
  WHERE transaction_timeline_events.description::text = 'Estimated closing day'::text
  ) ptcl 
  ON lhs.timeline_id = ptcl.transaction_timeline_id;

  
CREATE OR REPLACE VIEW datasci_projects.vw_users_property_tx_with_first_timeline_dates AS
  SELECT pea.id as token_id
    , pea.user_id
    , pea.email
    , pea.created_at as token_created_at
    , pea.scan_state
    , pea.state as user_state
    , pea.folio_enabled
    , pu.sign_in_count
    , pu.created_at as user_created_at
    , pu.type as user_type
    , pu.plaintext_invitation_token
--   , pu.invitation_sent_at
--   , pu.invitation_accepted_at
--   , pu.invited_by_id
    , pu.utm_source
    , pu.utm_medium
    , pu.utm_campaign
    , pu.referer
    , pu.signup_method
    , pu.sms_phone
    , pu.user_role
    , pu.k_factor
    , pk.invited_count
    , pk.converted_count
    , dspt.id as event_id
    , dspt.property_transaction_id  
    , dspt.type
    , dspt.created_at as tx_created_at
    , dspt.event_data
    , dspt.street_addr
    , dspt.city
    , dspt.state
    , dspt.zipcode
    , dspt.address
    , dspt.lat
    , dspt.lon
    , dspt.originating_message_id
    , dspt.email_classification_score
    , dspt.email_classification_version
    , dspt.deletion_method
    , dspt.rejection_reason
    , dspt.rejection_notes
    , dspt.mindate
    , dspt.tx_num
   FROM public.external_accounts pea
   LEFT JOIN public.users pu
    ON pea.user_id = pu.id
   LEFT JOIN (
    SELECT pk_1.inviter_id
      , SUM(pk_1.invited_count) AS invited_count
          , SUM(CASE WHEN pk_1.converted_at IS NULL THEN 0
                     ELSE 1
                  END) AS converted_count
      FROM public.k_factor_referrals pk_1
      GROUP BY pk_1.inviter_id) pk 
      ON pu.id = pk.inviter_id
   LEFT JOIN (
    SELECT dspt_1.*
      , dstte.mindate
      , ROW_NUMBER() OVER (PARTITION BY dspt_1.external_account_id ORDER BY dspt_1.created_at) AS tx_num
    FROM datasci.property_transaction_events dspt_1
    LEFT JOIN (
      SELECT dsptt.property_transaction_id
        , MIN(dsptt.created_at) AS mindate
          FROM datasci.property_transaction_timelines_ds dsptt
          WHERE (dsptt.type <> ALL (ARRAY['PropertyTransactionSharedEvent'::text, 'TransactionTimelineEventDestroyedEvent'::text])) 
            AND dsptt.transaction_timeline_event_date IS NOT NULL
          GROUP BY dsptt.property_transaction_id
          ) dstte ON dspt_1.property_transaction_id = dstte.property_transaction_id
    WHERE dspt_1.type IN ('PropertyTransactionConfirmedEvent', 'PropertyTransactionManuallyCreatedEvent')
    ) dspt
    ON pea.id = dspt.external_account_id::integer;

CREATE OR REPLACE VIEW datasci.vw_messages_and_document_dupes AS
 SELECT 'documents'::text AS "table",
    dstd.property_transaction_id,
    dstd.message_id,
    dstd.attachment_name,
    min(dstd.created_at) AS min_date,
    max(dstd.created_at) AS max_date,
    count(*) AS counter
   FROM datasci.transaction_documents dstd
  GROUP BY dstd.property_transaction_id, dstd.message_id, dstd.attachment_name
UNION
 SELECT 'messages'::text AS "table",
    dstm.property_transaction_id,
    dstm.message_id,
    NULL::character varying AS attachment_name,
    min(dstm.created_at) AS min_date,
    max(dstm.created_at) AS max_date,
    count(*) AS counter
   FROM datasci.transaction_messages dstm
  GROUP BY dstm.property_transaction_id, dstm.message_id
UNION
 SELECT 'query'::text AS "table",
    dsqc.property_transaction_id,
    dsqc.message_id,
    NULL::character varying AS attachment_name,
    min(dsqc.created_at) AS min_date,
    max(dsqc.created_at) AS max_date,
    count(*) AS counter
   FROM datasci.query_comparison dsqc
  GROUP BY dsqc.property_transaction_id, dsqc.message_id;


CREATE OR REPLACE VIEW datasci_projects.vw_transaction_timeline_dates AS
 WITH timeline_dates AS (
         SELECT row_number() OVER (PARTITION BY a_1.external_account_id, a_1.property_transaction_id ORDER BY b_1.transaction_timeline_event_date) AS "row",
            a_1.property_transaction_id,
            a_1.created_at,
            b_1.street_addr,
            a_1.external_account_id,
            b_1.transaction_timeline_event_id,
            a_1.transaction_timeline_event_description,
            b_1.transaction_timeline_event_date
           FROM ( SELECT property_transaction_timelines_ds.property_transaction_id,
                    max(property_transaction_timelines_ds.id) AS id,
                    max(property_transaction_timelines_ds.created_at) AS created_at,
                    property_transaction_timelines_ds.external_account_id,
                    property_transaction_timelines_ds.transaction_timeline_event_description
                   FROM datasci.property_transaction_timelines_ds
                  WHERE (property_transaction_timelines_ds.type = ANY (ARRAY['TransactionTimelineEventCreatedEvent'::text, 'TransactionTimelineEventUpdatedEvent'::text, 'TransactionTimelineEventDestroyedEvent'::text])) AND (property_transaction_timelines_ds.transaction_timeline_event_description = ANY (ARRAY['Estimated closing day'::text, 'Offer accepted'::text, 'Listing date'::text, 'Inspections deadline'::text, 'Earnest Money Due'::text, 'Inspection'::text, 'Loan deadline'::text, 'Inspection Contingency'::text, 'Appraisal'::text, 'Deposit'::text, 'Final walkthrough'::text]))
                  GROUP BY property_transaction_timelines_ds.property_transaction_id, property_transaction_timelines_ds.transaction_timeline_event_description, property_transaction_timelines_ds.external_account_id) a_1
             LEFT JOIN ( SELECT DISTINCT property_transaction_timelines_ds.id,
                    property_transaction_timelines_ds.transaction_timeline_event_id,
                    property_transaction_timelines_ds.street_addr,
                        CASE
                            WHEN property_transaction_timelines_ds.transaction_timeline_event_date = ''::text THEN NULL::date
                            ELSE property_transaction_timelines_ds.transaction_timeline_event_date::date
                        END AS transaction_timeline_event_date
                   FROM datasci.property_transaction_timelines_ds) b_1 ON a_1.id = b_1.id
        )
 SELECT a.property_transaction_id,
    c.street_addr,
    a.external_account_id,
    a.transaction_timeline_event_description AS event,
    a.transaction_timeline_event_date AS date,
    b.transaction_timeline_event_description AS following_event,
    b.transaction_timeline_event_date AS following_date
   FROM timeline_dates a
     LEFT JOIN timeline_dates b ON a.property_transaction_id = b.property_transaction_id AND a.external_account_id = b.external_account_id AND a."row" = (b."row" - 1)
     LEFT JOIN ( SELECT DISTINCT timeline_dates.property_transaction_id,
            timeline_dates.street_addr,
            timeline_dates."row"
           FROM timeline_dates
          WHERE timeline_dates."row" = 1) c ON c.property_transaction_id = a.property_transaction_id;


