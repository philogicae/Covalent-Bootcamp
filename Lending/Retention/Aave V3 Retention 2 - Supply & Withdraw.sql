WITH aave_supply AS (
    SELECT
        [signed_at:aggregation] AS date,
        chain_name AS market,
        count() AS supply
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND topic0 == unhex('2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61') -- supply
        AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
        AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
    GROUP BY date, market
), aave_withdraw AS (
    SELECT
        [signed_at:aggregation] AS date,
        chain_name AS market,
        count() AS withdraw
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND topic0 == unhex('3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7') -- withdraw
        AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
        AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
    GROUP BY date, market
)
SELECT
    if(notEmpty(asu.market), asu.date, awi.date) AS date,
    if(notEmpty(asu.market), asu.market, awi.market) AS market,
    asu.supply,
    awi.withdraw,
    asu.supply - awi.withdraw AS ratio
FROM aave_supply asu
FULL OUTER JOIN aave_withdraw awi
ON asu.date = awi.date AND asu.market = awi.market