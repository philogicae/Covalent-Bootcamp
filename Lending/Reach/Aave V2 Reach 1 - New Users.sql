WITH aave_users AS (
    SELECT
        min([signed_at:aggregation]) as date,
        chain_name AS market,
        tx_sender
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
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
    GROUP BY market, tx_sender
)
SELECT
    date,
    market,
    uniq(tx_sender) AS new_users
FROM aave_users
WHERE [date:daterange]
GROUP BY date, market