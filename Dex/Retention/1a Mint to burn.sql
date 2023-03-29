WITH add_liq AS (
    SELECT
        [signed_at:aggregation] AS date,
        sum(amount1_unscaled) AS eth
    FROM reports.dex
    WHERE chain_id = 42161
        AND [signed_at:daterange]
        AND pair_address = unhex('515e252b2b5c22b4b2b6Df66c2eBeeA871AA4d69')
        AND event = 'add_liquidity'
    GROUP BY date
), remove_liq AS (
    SELECT
        [signed_at:aggregation] AS date,
        sum(amount1_unscaled) AS eth
    FROM reports.dex
    WHERE chain_id = 42161
        AND [signed_at:daterange]
        AND pair_address = unhex('515e252b2b5c22b4b2b6Df66c2eBeeA871AA4d69')
        AND event = 'remove_liquidity'
    GROUP BY date
), prices AS (
    SELECT
        [signed_at:aggregation] AS date,
        avg(price_in_usd) AS eth_price
    FROM reports.token_prices
    WHERE chain_id = 42161
        AND [signed_at:daterange]
        AND contract_address = lower('82aF49447D8a07e3bd95BD0d56f35241523fBab1')
    GROUP BY date
)

SELECT
    p.date,
    'WBTC<>WETH' AS pair_ticker,
    (al.eth - rl.eth) / pow(10, 18) AS mint_to_burn,
    eth_price
FROM prices p
LEFT JOIN add_liq al
    ON p.date = al.date
LEFT JOIN remove_liq rl
    ON p.date = rl.date