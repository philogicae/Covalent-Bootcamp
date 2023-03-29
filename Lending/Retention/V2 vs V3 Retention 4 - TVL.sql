WITH aave_v2 AS (
    SELECT
        signed_at,
        chain_name,
        extract_address(hex(topic1)) as token_address,
        if(
            topic0 == unhex('de6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951'),
            to_float64_raw(data1),
            -1.0 * to_float64_raw(data1)
        ) as token_amount 
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
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
), aave_v2_tvl AS (
    SELECT
        [signed_at:aggregation] AS signed_at,
        'V2' AS version,
        sum(sum(token_amount/pow(10, num_decimals)*price_in_usd)) OVER (ORDER BY signed_at) AS TVL
    FROM aave_v2 a
    LEFT JOIN reports.token_prices p
        ON p.contract_address =
            CASE -- missing tokens on coingecko
                WHEN a.token_address = '5C49B268C9841AFF1CC3B0A418FF5C3442EE3F3B' THEN '8d6cebd76f18e1558d4db88138e2defb3909fad6'
                ELSE lower(a.token_address)
            END
        AND p.dt = date_trunc('day', a.signed_at)
    GROUP BY signed_at
), aave_v3 AS (
    SELECT
        signed_at,
        chain_name,
        extract_address(hex(topic1)) as token_address,
        if(
            topic0 == unhex('2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61'),
            to_float64_raw(data1),
            -1.0 * to_float64_raw(data1)
        ) as token_amount 
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND (
                topic0 == unhex('2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61') -- supply
            OR topic0 == unhex('3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7') -- withdraw
        )
        AND chain_id in [137, 43114, 42161, 10, 250, 1666600000]
        AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD')
        AND notEmpty(data1)
), aave_v3_tvl AS (
    SELECT
        [signed_at:aggregation] AS signed_at,
        'V3' AS version,
        sum(sum(token_amount/pow(10, num_decimals)*price_in_usd)) OVER (ORDER BY signed_at) AS TVL
    FROM aave_v3 a
    LEFT JOIN reports.token_prices p
        ON p.contract_address =
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
    GROUP BY signed_at
)

SELECT * FROM aave_v2_tvl
UNION ALL
SELECT * FROM aave_v3_tvl