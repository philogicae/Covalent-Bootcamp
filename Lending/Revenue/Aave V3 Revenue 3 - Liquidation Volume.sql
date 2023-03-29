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
        AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
        AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD') -- V3
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
            WHEN a.token_address = 'BA5DDD1F9D7F570DC94A51479A000E3BCE967196' THEN '7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
            WHEN a.token_address = 'D22A58F79E9481D1A88E00C343885A588B34B68B' THEN 'db25f211ab05b1c97d595516f45794528a807ad8'
            WHEN a.token_address = 'DA10009CBD5D07DD0CECC66161FC93D7C9000DA1' THEN '6b175474e89094c44da98b954eedeac495271d0f'
            WHEN a.token_address = '350A791BFC2C21F9ED5D10980DAD2E2638FFA7F6' THEN '514910771af9ca656af840dff83e8264ecf986ca'
            WHEN a.token_address = '7F5C764CBC14F9669B88837CA1490CCA17C31607' THEN 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
            WHEN a.token_address = '68F180FCCE6836688E9084F035309E29BF0A2095' THEN '2260fac5e5542a773aa44fbcfedf7c193bc2c599'
            WHEN a.token_address = '4200000000000000000000000000000000000006' THEN 'c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            WHEN a.token_address = '94B008AA00579C1307B0EF2C499AD98A8CE58E58' THEN 'dac17f958d2ee523a2206206994597c13d831ec7'
            WHEN a.token_address = '76FB31FB4AF56892A25E32CFC43DE717950C9278' THEN '7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
            WHEN a.token_address = '8C6F28F2F1A3C87F0F938B96D27520D9751EC8D9' THEN '57ab1ec28d129707052df4df418d58a2d46d5f51'
            ELSE lower(a.token_address)
        END
    AND p.dt = date_trunc('day', a.signed_at)
WHERE token_amount/pow(10, num_decimals)*price_in_usd < 50000000
GROUP BY date, chain_name