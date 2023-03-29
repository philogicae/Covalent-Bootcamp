WITH aave_collector AS (
    SELECT
        signed_at,
        chain_name,
        hex(log_emitter) AS token_address,
        to_float64_raw(data0) AS token_amount
    FROM blockchains.all_chains
    WHERE topic0 == unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') -- transfer
        AND (
                (chain_id == 137
                AND topic2 == unhex('000000000000000000000000e8599f3cc5d38a9ad6f3684cd5cea72f10dbc383')) -- Polygon V3
            OR (chain_id == 43114
                AND topic2 == unhex('0000000000000000000000005ba7fd868c40c16f7adfae6cf87121e13fc2f7a0')) -- Avalanche V3
            OR (chain_id == 42161
                AND topic2 == unhex('000000000000000000000000053d55f9b5af8694c503eb288a1b7e552f590710')) -- Arbitrum V3
            OR (chain_id == 10
                AND topic2 == unhex('000000000000000000000000b2289e329d2f85f1ed31adbb30ea345278f21bcf')) -- Optimism V3
            OR (chain_id == 250
                AND topic2 == unhex('000000000000000000000000be85413851d195fc6341619cd68bfdc26a25b928')) -- Fantom V3
            OR (chain_id == 1666600000
                AND topic2 == unhex('0000000000000000000000008a020d92d6b119978582be4d3edfdc9f7b28bf31')) -- Harmony V3
        )
), atokens AS (
    SELECT
        chain_name,
        extract_address(hex(topic2)) AS atoken,
        extract_address(hex(topic1)) AS asset
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND topic0 == unhex('3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f') -- ReserveInitialized
        AND -- LendingPoolConfigurator / PoolConfigurator
            chain_id in [137, 43114, 42161, 10, 250, 1666600000]
        AND log_emitter == unhex('8145eddDf43f50276641b55bd3AD95944510021E') -- V3
), atoken_prices AS (
    SELECT
        dt,
        a.chain_name,
        atoken,
        asset,
        price_in_usd,
        if(num_decimals > 0, num_decimals, 18) AS num_decimals
    FROM atokens a
    LEFT JOIN reports.token_prices p
        ON p.contract_address = 
            CASE -- missing tokens on coingecko
                WHEN a.asset = '5C49B268C9841AFF1CC3B0A418FF5C3442EE3F3B' THEN '8d6cebd76f18e1558d4db88138e2defb3909fad6'
                WHEN a.asset = 'BA5DDD1F9D7F570DC94A51479A000E3BCE967196' THEN '7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
                WHEN a.asset = 'D22A58F79E9481D1A88E00C343885A588B34B68B' THEN 'db25f211ab05b1c97d595516f45794528a807ad8'
                WHEN a.asset = 'DA10009CBD5D07DD0CECC66161FC93D7C9000DA1' THEN '6b175474e89094c44da98b954eedeac495271d0f'
                WHEN a.asset = '350A791BFC2C21F9ED5D10980DAD2E2638FFA7F6' THEN '514910771af9ca656af840dff83e8264ecf986ca'
                WHEN a.asset = '7F5C764CBC14F9669B88837CA1490CCA17C31607' THEN 'a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
                WHEN a.asset = '68F180FCCE6836688E9084F035309E29BF0A2095' THEN '2260fac5e5542a773aa44fbcfedf7c193bc2c599'
                WHEN a.asset = '4200000000000000000000000000000000000006' THEN 'c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
                WHEN a.asset = '94B008AA00579C1307B0EF2C499AD98A8CE58E58' THEN 'dac17f958d2ee523a2206206994597c13d831ec7'
                WHEN a.asset = '76FB31FB4AF56892A25E32CFC43DE717950C9278' THEN '7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
                WHEN a.asset = '8C6F28F2F1A3C87F0F938B96D27520D9751EC8D9' THEN '57ab1ec28d129707052df4df418d58a2d46d5f51'
                ELSE lower(a.asset)
            END
        --AND p.dt = (SELECT max(dt) from reports.token_prices)
    WHERE [signed_at:daterange]
), aave_prices AS (
    SELECT dt, chain_name, atoken AS token_address, price_in_usd, num_decimals FROM atoken_prices
    UNION ALL
    SELECT dt, chain_name, asset AS token_address, price_in_usd, num_decimals FROM atoken_prices
)

SELECT
    [signed_at:aggregation] AS date,
    chain_name,
    sumIf(token_amount/pow(10, num_decimals)*price_in_usd, token_amount/pow(10, num_decimals)*price_in_usd < 50000) AS fees
    --sum(sum((token_amount/pow(10, num_decimals)*price_in_usd))) OVER (PARTITION BY chain_name ORDER BY date) AS TVL
FROM aave_collector ac
LEFT JOIN aave_prices ap ON ap.token_address = ac.token_address AND ap.chain_name = ac.chain_name
    AND date_trunc('day', signed_at) = ap.dt
WHERE [signed_at:daterange]
GROUP BY date, chain_name