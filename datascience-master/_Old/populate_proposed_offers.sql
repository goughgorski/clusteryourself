TRUNCATE TABLE datasci_mining.proposed_purchase_offers;
INSERT INTO datasci_mining.proposed_purchase_offers (event_id)
    SELECT dspt.id
    FROM datasci.property_transactions_ds dspt
    LEFT JOIN datasci_mining.offer_messages dsmom
            ON dspt.id = dsmom.event_id
    LEFT JOIN datasci_mining.proposed_purchase_offers dsmppo
            ON dspt.id = dsmppo.event_id
    LEFT JOIN datasci_projects.dupes_property_transaction_confirmed_and_rejected dsptcr
        ON (dspt.type = 'PropertyTransactionConfirmedEvent' AND dspt.property_transaction_id = dsptcr.property_transaction_id::integer)             
    LEFT JOIN datasci_mining.offers_over_message_threshold dsmoomt
        ON (dspt.id = dsmoomt.event_id AND dspt.property_transaction_id = dsmoomt.property_transaction_id AND dsmoomt.messages_count > 100)         
    WHERE dspt.type IN ('PropertyTransactionConfirmedEvent')
    AND dspt.state IN (SELECT DISTINCT state FROM datasci_mining.document_parsers)
    -- AND dspt.created_at >= '6-1-2017'
    AND dsptcr.property_transaction_id IS NULL
    AND dsmppo.event_id IS NULL
    AND dsmom.event_id IS NULL
    AND dsmoomt.event_id IS NULL
    AND dspt.id NOT IN (1826702,15209220);
