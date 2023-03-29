SELECT
    [signed_at:aggregation] AS date,
    chain_name,
    sum(token_amount/pow(10, num_decimals)*price_in_usd) AS TVL_change
FROM (
    SELECT
        signed_at,
        chain_name,
        extract_address(hex(topic1)) AS token_address,
        CASE
            WHEN topic0 == unhex('de6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951')
            THEN to_float64_raw(data1)
            ELSE -1.0 * to_float64_raw(data1)
        END AS token_amount
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND [signed_at:daterange]
        AND (
                topic0 == unhex('de6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951') -- deposit
            OR topic0 == unhex('3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7') -- withdraw
        )
        AND (
                (chain_id == 1
                AND log_emitter == unhex('7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')) -- Mainnet V2
            OR (chain_id == 137
                AND log_emitter == unhex('8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf')) -- Polygon V2
            OR (chain_id == 43114
                AND log_emitter == unhex('4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C')) -- Avalanche V2
        ) AND notEmpty(data1)
) a
LEFT JOIN (
        SELECT DISTINCT ON (dt, contract_address)
            dt, contract_address, price_in_usd,
            CASE WHEN num_decimals > 0 THEN num_decimals ELSE 18 END AS num_decimals
        FROM reports.token_prices prices
        WHERE [chain_name:chainname]
            AND [signed_at:daterange]
) p ON p.contract_address =
        CASE -- missing tokens on coingecko
            WHEN a.token_address = '5C49B268C9841AFF1CC3B0A418FF5C3442EE3F3B' THEN '8d6cebd76f18e1558d4db88138e2defb3909fad6'
            ELSE lower(a.token_address)
        END
    AND p.dt = date_trunc('day', a.signed_at)
GROUP BY date, chain_name