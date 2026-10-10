SELECT e.canonical_id,
  JSON_EXISTS(e.payload, '$.traits.name') AS name_present,
  JSON_VALUE(e.payload, '$.traits.name') AS name,
  JSON_VALUE(e.payload, '$.traits.quality_score'
             RETURNING DOUBLE PRECISION) AS score,
  JSON_VALUE(e.payload, '$.traits.steam_wishlisted'
             RETURNING BOOLEAN) AS wishlisted,
  JSON_VALUE(e.payload,
    '$.identifiers[*] ? (@.identity_type == "email").value') AS email,
  JSON_QUERY(e.payload, '$.traits.linked_accounts') AS accounts,
  JSON_OBJECT('source' VALUE 'google', 'medium' VALUE '',
    'accounts' VALUE JSON_QUERY(e.payload, '$.traits.linked_accounts')
      FORMAT JSON) AS coherent_payload
FROM (VALUES ('p1', '{"traits":{"name":null,"quality_score":0,"steam_wishlisted":false,"linked_accounts":{"discord":{"external_id":"d1"}}},"identifiers":[{"identity_type":"email","value":"a@example.test"}]}'))
  AS e(canonical_id, payload);
