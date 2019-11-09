SELECT pt.id
	, pt.address_id
    , a.street_addr
    , a.city
    , a.state
    , a.zipcode
    , a.latitude
    , a.longitude
    , a.unit
    , pt.created_at
    , pt.updated_at
    , pt.status
    , pt.manually_created
    , pt.pending_initial_import
    , pt.archived
    , pt.has_street_view
    , pt.setup_dismissed
    , pt.agent_represents_buyer
    , pt.agent_represents_seller
    , pt.shared_with_client_at
    , pt.email_count
    , pt.timeline_id
    , pt.creation_method
    , pt.email_classification_score
    , tt.created_at as timeline_created_at
    , tt.updated_at as timeline_updated_at
    , tt.modified as timeline_modified
    , tte.timeline_event_count
    , doc.transaction_doc_count
FROM (SELECT pt.id
        , pt.address_id
        , pt.created_at
        , pt.updated_at
        , pt.status
        , pt.manually_created
        , pt.pending_initial_import
        , pt.archived
        , pt.has_street_view
        , pt.setup_dismissed
        , pt.agent_represents_buyer
        , pt.agent_represents_seller
        , pt.shared_with_client_at
        , pt.email_count
        , pt.timeline_id
        , pt.creation_method
        , pt.email_classification_score
      FROM property_transactions pt
      WHERE (CURRENT_TIMESTAMP - created_at) < '31 days') pt
LEFT JOIN addresses a 
	ON pt.address_id = a.id
LEFT JOIN transaction_timelines tt
	ON pt.timeline_id = tt.id
LEFT JOIN (SELECT transaction_timeline_id 
			, SUM(1) as timeline_event_count 
		FROM transaction_timeline_events GROUP BY transaction_timeline_id) tte
     ON tt.id = tte.transaction_timeline_id
LEFT JOIN (SELECT property_transaction_id
 			, SUM(1) as transaction_doc_count
		FROM documents GROUP BY property_transaction_id) doc
	ON pt.id = doc.property_transaction_id
