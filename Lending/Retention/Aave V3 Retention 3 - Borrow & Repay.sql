WITH aave_supply AS (
    SELECT
        [signed_at:aggregation] AS date,
        chain_name AS market,
        count() AS borrow
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND topic0 == unhex('b3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0') -- borrow
        AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
        AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
    GROUP BY date, market
), aave_withdraw AS (
    SELECT
        [signed_at:aggregation] AS date,
        chain_name AS market,
        count() AS repay
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND topic0 == unhex('a534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051') -- repay
        AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
        AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
    GROUP BY date, market
)
SELECT
    if(notEmpty(asu.market), asu.date, awi.date) AS date,
    if(notEmpty(asu.market), asu.market, awi.market) AS market,
    asu.borrow,
    awi.repay,
    asu.borrow - awi.repay AS ratio
FROM aave_supply asu
FULL OUTER JOIN aave_withdraw awi
ON asu.date = awi.date AND asu.market = awi.market