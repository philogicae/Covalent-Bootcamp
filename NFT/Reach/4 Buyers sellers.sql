SELECT [signed_at:aggregation] AS date, uniq(maker) AS users, 'sellers' AS label
FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
        AND [signed_at:daterange]
        AND market = 'aavegotchi' 
GROUP BY date

UNION ALL

SELECT [signed_at:aggregation] AS date, uniq(taker) AS users, 'buyers' AS label
FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
        AND [signed_at:daterange]
        AND market = 'aavegotchi' 
GROUP BY date