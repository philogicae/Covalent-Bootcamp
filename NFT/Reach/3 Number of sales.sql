SELECT
    [signed_at:aggregation] as date,
    count(tx_hash) AS sales
FROM reports.nft_sales_all_chains
WHERE chain_name = 'matic_mainnet'
    AND [signed_at:daterange]
    AND market = 'aavegotchi'
    AND hex(collection_address) != 'A02D547512BB90002807499F05495FE9C4C3943F'
GROUP BY date