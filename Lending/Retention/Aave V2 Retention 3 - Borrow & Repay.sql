WITH aave_borrow AS (
    SELECT
        [signed_at:aggregation] AS date,
        chain_name AS market,
        count() AS borrow
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND topic0 == unhex('c6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b') -- borrow
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
), aave_repay AS (
    SELECT
        [signed_at:aggregation] AS date,
        chain_name AS market,
        count() AS repay
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND topic0 == unhex('4cdde6e09bb755c9a5589ebaec640bbfedff1362d4b255ebf8339782b9942faa') -- repay
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
    if(notEmpty(abo.market), abo.date, arep.date) AS date,
    if(notEmpty(abo.market), abo.market, arep.market) AS market,
    abo.borrow,
    arep.repay,
    abo.borrow - arep.repay AS ratio
FROM aave_borrow abo
FULL OUTER JOIN aave_repay arep
ON abo.date = arep.date AND abo.market = arep.market