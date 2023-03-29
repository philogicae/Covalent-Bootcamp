SELECT
    [signed_at:aggregation] AS date,
    chain_name,
    sum(token_amount/pow(10, num_decimals)*price_in_usd) AS liquidated
FROM (
    SELECT
        signed_at,
        chain_name,
        extract_address(hex(topic2)) AS token_address,
        to_float64_raw(data0) AS token_amount 
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND topic0 == unhex('e413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286') -- liquidationCall
        AND (
                (chain_id == 1
                AND log_emitter == unhex('7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')) -- Mainnet V2
            OR (chain_id == 137
                AND log_emitter == unhex('8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')) -- Polygon V2
            OR (chain_id == 43114
                AND log_emitter == unhex('4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')) -- Avalanche V2
        )
) a
LEFT JOIN (
        SELECT DISTINCT ON (dt, contract_address)
            dt, contract_address, price_in_usd,
            CASE WHEN num_decimals > 0 THEN num_decimals ELSE 18 END AS num_decimals
        FROM reports.token_prices prices
        WHERE [signed_at:daterange]
) p ON p.contract_address =
        CASE -- missing tokens on coingecko
            WHEN a.token_address = '5C49B268C9841AFF1CC3B0A418FF5C3442EE3F3B' THEN '8d6cebd76f18e1558d4db88138e2defb3909fad6'
            ELSE lower(a.token_address)
        END
    AND p.dt = date_trunc('day', a.signed_at)
WHERE token_amount/pow(10, num_decimals)*price_in_usd < 50000000
GROUP BY date, chain_name