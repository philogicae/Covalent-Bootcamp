SELECT
    [signed_at:aggregation] AS date,
    chain_name AS market,
    count() AS tx
FROM blockchains.all_chains
WHERE [chain_name:chainname]
    AND [signed_at:daterange]
    AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
    AND tx_recipient == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
GROUP BY date, market
ORDER BY date DESC