WITH users AS (
    SELECT
        distinct taker,
        [signed_at:aggregation] AS date
    FROM reports.nft_sales_all_chains
        WHERE chain_name = 'matic_mainnet'
            AND [signed_at:daterange]
            AND market = 'aavegotchi'
)

SELECT
    sum(nft_token_price_usd) AS volume,
    [signed_at:aggregation] AS date,
    market
FROM reports.nft_sales_all_chains a
INNER JOIN users u ON a.taker = u.taker
WHERE chain_name = 'matic_mainnet'
    AND [signed_at:daterange]
GROUP BY date, market