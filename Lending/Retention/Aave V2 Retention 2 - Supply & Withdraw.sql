WITH aave_supply AS (
    SELECT
        [signed_at:aggregation] AS date,
        chain_name AS market,
        count() AS supply
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND topic0 == unhex('de6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951') -- deposit
        AND (
                (chain_id == 1
                AND log_emitter == unhex('7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')) -- Mainnet V2
            OR
                (chain_id == 137
                AND log_emitter == unhex('8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')) -- Polygon V2
            OR
                (chain_id == 43114
                AND log_emitter == unhex('4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')) -- Avalanche V2
        )
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
        AND (
                (chain_id == 1
                AND log_emitter == unhex('7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')) -- Mainnet V2
            OR
                (chain_id == 137
                AND log_emitter == unhex('8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')) -- Polygon V2
            OR
                (chain_id == 43114
                AND log_emitter == unhex('4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')) -- Avalanche V2
        )
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