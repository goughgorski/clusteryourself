CREATE OR REPLACE VIEW datasci_modeling.vw_event_counts_user_month AS
 SELECT dspte.external_account_id
 , pea.user_id
 , dspte.month
 , CASE
    WHEN dspte_confirmed.confirmed IS NULL THEN 0::bigint
    ELSE dspte_confirmed.confirmed
  END AS confirmed
 , CASE
    WHEN dspte_rejected.rejected IS NULL THEN 0::bigint
    ELSE dspte_rejected.rejected
  END AS rejected
 , CASE
    WHEN dspte_manually.manually_created IS NULL THEN 0::bigint
    ELSE dspte_manually.manually_created
  END AS manually_created
 , CASE
    WHEN dspte_auto.auto_created IS NULL THEN 0::bigint
    ELSE dspte_auto.auto_created
  END AS auto_created
 , CASE
    WHEN dsptt_shared.shared IS NULL THEN 0::bigint
    ELSE dsptt_shared.shared
  END AS shared
 , dspte.start_date
 , pea.type
 , pu.signup_method
 , CASE
    WHEN ps.id IS NOT NULL AND ps.subscription_canceled_on IS NULL THEN 1
    ELSE 0
  END AS subscribed
 , date_part('day'::text, date_trunc('month'::text, dspte.start_date) + '1 mon'::interval - dspte.start_date) AS start_month_length
 FROM ( SELECT DISTINCT a.external_account_id
  , generate_series(0, a.length::integer) AS month
  , a.start_date
    FROM ( SELECT DISTINCT dspte_1.external_account_id
      , dspte_1.created_at
      , MIN(dspte_1.created_at) OVER (PARTITION BY dspte_1.external_account_id) AS start_date
      , DATE_PART('month'::text, NOW()::date) - DATE_PART('month'::text, MIN(dspte_1.created_at) 
        OVER (PARTITION BY dspte_1.external_account_id)) 
      + (DATE_PART('year'::text, NOW()::date) - DATE_PART('year'::text, MIN(dspte_1.created_at) 
        OVER (PARTITION BY dspte_1.external_account_id))) * 12::double precision AS length
  FROM datasci.property_transaction_events dspte_1
  LEFT JOIN external_accounts pea_1 ON pea_1.id = dspte_1.external_account_id
  WHERE dspte_1.external_account_id IS NOT NULL AND pea_1.id IS NOT NULL AND dspte_1.created_at > '2017-11-20 00:00:00-08'::timestamp with time zone
    ) a
 ) dspte
 LEFT JOIN ( SELECT dspte_1.external_account_id
  , DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
  + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision AS month
  , COUNT(dspte_1.external_account_id) AS confirmed
  FROM ( SELECT dspte_2.external_account_id
    , dspte_2.type
    , dspte_2.created_at
    , MIN(dspte_2.created_at) OVER (PARTITION BY dspte_2.external_account_id) AS start_date
    FROM datasci.property_transaction_events dspte_2
    LEFT JOIN external_accounts pea_1 ON pea_1.id = dspte_2.external_account_id
    WHERE dspte_2.external_account_id IS NOT NULL AND dspte_2.created_at > '2017-11-20 00:00:00-08'::timestamp with time zone
    ) dspte_1
  WHERE dspte_1.type = 'PropertyTransactionConfirmedEvent'::text
  GROUP BY dspte_1.external_account_id
  , (DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
    + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision)
  ) dspte_confirmed 
 ON dspte.external_account_id = dspte_confirmed.external_account_id AND dspte.month::double precision = dspte_confirmed.month
 LEFT JOIN ( SELECT dspte_1.external_account_id
  , DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
  + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision AS month
  , COUNT(dspte_1.external_account_id) AS rejected
  FROM ( SELECT dspte_2.external_account_id
    , dspte_2.type
    , dspte_2.created_at
    , MIN(dspte_2.created_at) OVER (PARTITION BY dspte_2.external_account_id) AS start_date
    FROM datasci.property_transaction_events dspte_2
    LEFT JOIN external_accounts pea_1 ON pea_1.id = dspte_2.external_account_id
    WHERE dspte_2.external_account_id IS NOT NULL AND dspte_2.created_at > '2017-11-20 00:00:00-08'::timestamp with time zone
    ) dspte_1
  WHERE dspte_1.type = 'PropertyTransactionRejectedEvent'::text
  GROUP BY dspte_1.external_account_id
  , (DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
    + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision)
  ) dspte_rejected
 ON dspte.external_account_id = dspte_rejected.external_account_id AND dspte.month::double precision = dspte_rejected.month
 LEFT JOIN ( SELECT dspte_1.external_account_id
  , DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
  + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision AS month
  , COUNT(dspte_1.external_account_id) AS manually_created
  FROM ( SELECT dspte_2.external_account_id
    , dspte_2.type
    , dspte_2.created_at
    , MIN(dspte_2.created_at) OVER (PARTITION BY dspte_2.external_account_id) AS start_date
    FROM datasci.property_transaction_events dspte_2
    LEFT JOIN external_accounts pea_1 ON pea_1.id = dspte_2.external_account_id
    WHERE dspte_2.external_account_id IS NOT NULL AND dspte_2.created_at > '2017-11-20 00:00:00-08'::timestamp with time zone
    ) dspte_1
  WHERE dspte_1.type = 'PropertyTransactionManuallyCreatedEvent'::text
  GROUP BY dspte_1.external_account_id
  , (DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
    + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision)
  ) dspte_manually 
 ON dspte.external_account_id = dspte_manually.external_account_id AND dspte.month::double precision = dspte_manually.month
 LEFT JOIN ( SELECT dspte_1.external_account_id
  , DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
  + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision AS month
  , COUNT(dspte_1.external_account_id) AS auto_created
  FROM ( SELECT dspte_2.external_account_id
    , dspte_2.type
    , dspte_2.created_at
    , MIN(dspte_2.created_at) OVER (PARTITION BY dspte_2.external_account_id) AS start_date
    FROM datasci.property_transaction_events dspte_2
    LEFT JOIN external_accounts pea_1 ON pea_1.id = dspte_2.external_account_id
    WHERE dspte_2.external_account_id IS NOT NULL AND dspte_2.created_at > '2017-11-20 00:00:00-08'::timestamp with time zone
    ) dspte_1
  WHERE dspte_1.type = 'PropertyTransactionAutomaticallyCreatedEvent'::text
  GROUP BY dspte_1.external_account_id
  , (DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
    + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision)
  ) dspte_auto
 ON dspte.external_account_id = dspte_auto.external_account_id AND dspte.month::double precision = dspte_auto.month
 LEFT JOIN ( SELECT dspte_1.external_account_id
  , DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
  + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision AS month
  , COUNT(dspte_1.external_account_id) AS shared
  FROM ( SELECT dspte_2.external_account_id
    , dsptt.type
    , dsptt.created_at
    , MIN(dspte_2.created_at) OVER (PARTITION BY dspte_2.external_account_id) AS start_date
    FROM datasci_testing.property_transaction_timelines_ds dsptt
    LEFT JOIN datasci.property_transaction_events dspte_2 ON dsptt.property_transaction_id = dspte_2.property_transaction_id
    LEFT JOIN external_accounts pea_1 ON pea_1.id = dspte_2.external_account_id
    WHERE dsptt.type = 'PropertyTransactionSharedEvent'::text 
    AND dspte_2.created_at > '2017-11-20 00:00:00-08'::timestamp with time zone 
    AND dsptt.created_at > '2017-11-20 00:00:00-08'::timestamp with time zone
    ) dspte_1
  GROUP BY dspte_1.external_account_id
  , (DATE_PART('month'::text, dspte_1.created_at) - DATE_PART('month'::text, dspte_1.start_date) 
    + (DATE_PART('year'::text, dspte_1.created_at) - DATE_PART('year'::text, dspte_1.start_date)) * 12::double precision)
  ) dsptt_shared
 ON dspte.external_account_id = dsptt_shared.external_account_id AND dspte.month::double precision = dsptt_shared.month
 LEFT JOIN external_accounts pea ON pea.id = dspte.external_account_id
 LEFT JOIN users pu ON pea.user_id = pu.id
 LEFT JOIN subscriptions ps ON pea.user_id = ps.user_id
 WHERE pu.type::text = 'AgentUser'::text;