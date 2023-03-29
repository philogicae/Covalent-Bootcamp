WITH aave_collector AS (
    SELECT
        signed_at,
        chain_name,
        hex(log_emitter) AS token_address,
        to_float64_raw(data0) AS token_amount
    FROM blockchains.all_chains
    WHERE topic0 == unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef') -- transfer
        AND (
                (chain_id == 1
                AND topic2 == unhex('000000000000000000000000464c71f6c2f760dda6093dcb91c24c39e5d6e18c')) -- Mainnet V2
            OR (chain_id == 137
                AND topic2 == unhex('0000000000000000000000007734280a4337f37fbf4651073db7c28c80b339e9')) -- Polygon V2
            OR (chain_id == 43114
                AND topic2 == unhex('000000000000000000000000467b92af281d14cb6809913ad016a607b5ba8a36')) -- Avalanche V2
        )
), atokens AS (
    SELECT
        chain_name,
        extract_address(hex(topic2)) AS atoken,
        extract_address(hex(topic1)) AS asset
    FROM blockchains.all_chains
    WHERE [chain_name:chainname]
        AND topic0 == unhex('3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f') -- ReserveInitialized
        AND ( -- LendingPoolConfigurator
               (chain_id == 1
                AND log_emitter == unhex('311Bb771e4F8952E6Da169b425E7e92d6Ac45756')) -- Mainnet V2
            OR (chain_id == 137
                AND log_emitter == unhex('26db2B833021583566323E3b8985999981b9F1F3')) -- Polygon V2
            OR (chain_id == 43114
                AND log_emitter == unhex('230B618aD4C475393A7239aE03630042281BD86e')) -- Avalanche V2
        )
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