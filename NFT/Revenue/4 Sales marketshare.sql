WITH all AS (
    SELECT
        [signed_at:aggregation] as date,
        sum(nft_token_price_usd) as volume
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
        AND [signed_at:daterange]
    GROUP BY date
), gotchi AS (
    SELECT
        [signed_at:aggregation] as date,
        sum(nft_token_price_usd) as volume
    FROM reports.nft_sales_all_chains  
    WHERE chain_name = 'matic_mainnet'
        AND [signed_at:daterange]
        AND market = 'aavegotchi'
    GROUP BY date
)

SELECT
    date,
    g.volume / a.volume AS sales
FROM all a
LEFT JOIN gotchi g ON a.date = g.date
ORDER BY date