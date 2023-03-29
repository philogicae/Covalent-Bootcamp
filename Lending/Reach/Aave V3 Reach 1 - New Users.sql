WITH aave_users AS (
    SELECT
        min([signed_at:aggregation]) as date,
        chain_name AS market,
        tx_sender
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
    AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
    AND tx_recipient == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
    GROUP BY market, tx_sender
)
SELECT
    date,
    market,
    uniq(tx_sender) AS new_users
FROM aave_users
WHERE [date:daterange]
GROUP BY date, market