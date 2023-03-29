WITH aave_v2_users AS (
    SELECT
        [signed_at:aggregation] as date,
        chain_name AS market,
        uniq(tx_sender) AS active_users
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND (
                (chain_id == 1
                AND tx_recipient == unhex('7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')) -- Mainnet V2
            OR
                (chain_id == 137
                AND tx_recipient == unhex('8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')) -- Polygon V2
            OR
                (chain_id == 43114
                AND tx_recipient == unhex('4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')) -- Avalanche V2
        )
    GROUP BY date, market
), aave_v3_users AS (
    SELECT
        [signed_at:aggregation] as date,
        chain_name AS market,
        uniq(tx_sender) AS active_users
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
        AND tx_recipient == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD') -- V3
    GROUP BY date, market
)

SELECT
    date,
    'V2' AS version,
    sum(active_users) AS active_users
FROM aave_v2_users
GROUP BY date

UNION ALL

SELECT
    date,
    'V3' AS version,
    sum(active_users) AS active_users
FROM aave_v3_users
GROUP BY date