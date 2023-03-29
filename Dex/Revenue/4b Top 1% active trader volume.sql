WITH top_traders AS (
    SELECT count, sender
    FROM (
        SELECT count(tx_hash) AS count, sender
        FROM reports.dex
        WHERE chain_id = 42161
        GROUP BY sender
        ORDER BY count DESC
    ) SQ1
    INNER JOIN (
        SELECT quantile(0.99)(count) AS quartile
        FROM (
            SELECT count(tx_hash) AS count, sender
            FROM reports.dex
            WHERE chain_id = 42161
            GROUP BY sender
            ORDER BY count DESC
        )
    ) SQ2 ON 1=1
    WHERE count > quartile
), volume_pool AS (
    SELECT
        [signed_at:aggregation] AS date,
        sum(abs(amount0_unscaled)/power(10, prices.num_decimals)*prices.price_in_usd) AS volume
    FROM reports.dex dex
    LEFT JOIN (
            SELECT dt, contract_address, price_in_usd, num_decimals
            FROM reports.token_prices 
            WHERE chain_id = 42161
        ) prices ON hex(dex.token0_address) = upper(prices.contract_address)
                    AND date_trunc('day', dex.signed_at) = prices.dt
    WHERE chain_id = 42161
        AND [signed_at:daterange]
        AND pair_address = unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
        AND event = 'swap'
    GROUP BY date
), volume_top_traders AS (
    SELECT
        [signed_at:aggregation] AS date,
        sum(abs(amount0_unscaled)/power(10, prices.num_decimals)*prices.price_in_usd) AS volume
    FROM (
        SELECT *
        FROM reports.dex dex
        INNER JOIN top_traders tt ON dex.sender = tt.sender
    ) dex
    LEFT JOIN (
            SELECT dt, contract_address, price_in_usd, num_decimals
            FROM reports.token_prices 
            WHERE chain_id = 42161
        ) prices ON hex(dex.token0_address) = upper(prices.contract_address)
                    AND date_trunc('day', dex.signed_at) = prices.dt
    WHERE chain_id = 42161
        AND [signed_at:daterange]
        AND pair_address = unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
        AND event = 'swap'
    GROUP BY date
)

SELECT
    date,
    vtt.volume / vp.volume AS top_traders
FROM volume_pool vp
LEFT JOIN volume_top_traders vtt on vp.date = vtt.date
