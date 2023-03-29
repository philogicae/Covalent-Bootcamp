SELECT
    [signed_at:aggregation] AS date,
    'V2' AS version,
    countIf(topic0 == unhex('de6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951')) AS supply,
    countIf(topic0 == unhex('3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7')) AS withdraw,
    supply - withdraw AS ratio
FROM blockchains.all_chains
WHERE [chain_name:chainname]
    AND [signed_at:daterange]
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
GROUP BY date

UNION ALL

SELECT
    [signed_at:aggregation] AS date,
    'V3' AS version,
    countIf(topic0 == unhex('2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61')) AS supply,
    countIf(topic0 == unhex('3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7')) AS withdraw,
    supply - withdraw AS ratio
FROM blockchains.all_chains
WHERE [chain_name:chainname]
    AND [signed_at:daterange]
    AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
    AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
GROUP BY date