SELECT
    [signed_at:aggregation] AS date,
    if(
        log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD'),
        'V3',
        'V2'
    ) AS version,
    count() AS tx
FROM blockchains.all_chains
WHERE [chain_name:chainname]
    AND [signed_at:daterange]
    AND topic0 == unhex('e413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286') -- liquidationCall
    AND (
            (chain_id == 1
            AND log_emitter == unhex('7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')) -- Mainnet V2
        OR
            (chain_id == 137
            AND log_emitter == unhex('8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')) -- Polygon V2
        OR
            (chain_id == 43114
            AND log_emitter == unhex('4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')) -- Avalanche V2
        OR
            (chain_id in [137, 43114, 42161, 10, 250, 1666600000]
            AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')) -- V3
    ) 
GROUP BY date, version