WITH users AS (
    SELECT
        [signed_at:aggregation] AS date,
        uniq(taker) AS users
    FROM reports.nft_sales_all_chains
        WHERE chain_name = 'matic_mainnet'
            AND [signed_at:daterange]
            AND market = 'aavegotchi' 
    GROUP BY date
), volume AS (
    SELECT
        [signed_at:aggregation] AS date,
        sum(nft_token_price_usd)*0.035 AS fee
    FROM reports.nft_sales_all_chains  
    WHERE chain_name = 'matic_mainnet'
        AND [signed_at:daterange]
        AND market = 'aavegotchi'
    GROUP BY date
)

SELECT date, Avg_fee_per_buyer, (Avg_fee_per_buyer/Previous_Period)-1 AS Growth_Rates
FROM (
    SELECT
        date,
        fee/users AS Avg_fee_per_buyer,
        lagInFrame(Avg_fee_per_buyer) OVER (ORDER BY date) AS Previous_Period
    FROM users u
    LEFT JOIN volume v ON u.date = v.date
)