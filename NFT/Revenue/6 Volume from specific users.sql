WITH buyers AS (
    SELECT count(tx_hash) AS count, taker AS buyer
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
    GROUP BY buyer
    ORDER BY count DESC
), top_buyers AS (
    SELECT count, buyer
    FROM buyers
    INNER JOIN (
        SELECT quantile(0.99)(count) AS quartile
        FROM buyers
    ) top ON 1=1
    WHERE count > quartile
), volume_market AS (
    SELECT
        [signed_at:aggregation] as date,
        sum(nft_token_price_usd) as volume
    FROM reports.nft_sales_all_chains  
    WHERE chain_name = 'matic_mainnet'
        AND [signed_at:daterange]
        AND market = 'aavegotchi'
    GROUP BY date
), volume_top_buyers AS (
    SELECT
        [signed_at:aggregation] as date,
        sum(nft_token_price_usd) as volume
    FROM reports.nft_sales_all_chains a
    INNER JOIN top_buyers t ON t.buyer = a.taker
    WHERE chain_name = 'matic_mainnet'
            AND [signed_at:daterange]
            AND market = 'aavegotchi'
    GROUP BY date
)

SELECT date, vtt.volume / vm.volume AS top_buyers
FROM volume_market vm
LEFT JOIN volume_top_buyers vtt ON vm.date = vtt.date