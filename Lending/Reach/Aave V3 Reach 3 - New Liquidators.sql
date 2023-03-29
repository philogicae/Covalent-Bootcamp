WITH aave_liquidators AS (
    SELECT
        min([signed_at:aggregation]) AS date,
        chain_name AS market,
        extract_address(hex(data2)) AS liquidator
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
    AND topic0 == unhex('e413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286') -- liquidationCall
    AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
    AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
    GROUP BY market, liquidator
)
SELECT
    date,
    market,
    uniq(liquidator) AS new_liquidators
FROM aave_liquidators
WHERE [date:daterange]
GROUP BY date, market