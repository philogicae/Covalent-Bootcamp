SELECT
    [signed_at:aggregation] AS date,
    'V2' AS version,
    countIf(topic0 == unhex('c6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b')) AS borrow,
    countIf(topic0 == unhex('4cdde6e09bb755c9a5589ebaec640bbfedff1362d4b255ebf8339782b9942faa')) AS repay,
    borrow - repay AS ratio
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
    countIf(topic0 == unhex('b3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0')) AS borrow,
    countIf(topic0 == unhex('a534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051')) AS repay,
    borrow - repay AS ratio
FROM blockchains.all_chains
WHERE [chain_name:chainname]
    AND [signed_at:daterange]
    AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
    AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD') -- V3
GROUP BY date