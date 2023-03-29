SELECT
    date_trunc('day', signed_at) as date,
    sum(nft_token_price_usd) as volume,
    CASE
        WHEN hex(collection_address) = '19F870BD94A34B3ADAA9CAA439D333DA18D6812A' THEN 'Installations'
        WHEN hex(collection_address) = '9216C31D8146BCB3EA5A9162DC1702E8AEDCA355' THEN 'Tiles'
        WHEN hex(collection_address) = 'A4E3513C98B30D4D7CC578D2C328BD550725D1D0' THEN 'FAKE Gotchis'
        WHEN hex(collection_address) = '9F6BCC63E86D44C46E85564E9383E650DC0B56D7' THEN 'FAKE Gotchi Cards'
        ELSE collection_name
    END AS collection_name
FROM reports.nft_sales_all_chains
WHERE chain_name = 'matic_mainnet'
    AND signed_at > now() - interval '30 day'
    AND market = 'aavegotchi'
    AND hex(collection_address) != 'A02D547512BB90002807499F05495FE9C4C3943F'
GROUP BY date, collection_name