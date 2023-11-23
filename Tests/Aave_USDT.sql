WITH aave_usdt AS (
    SELECT
        signed_at,
        concat('0x', hex(tx_hash)) AS tx_hash,
        concat('0x', hex(tx_sender)) AS sender,
        topic0,
        data0,
        data1
        FROM blockchains.all_chains
        WHERE chain_id == 137
            AND [signed_at:daterange]
            AND log_emitter == unhex('794a61358D6845594F94dc1DB02A252b5b4814aD') -- pool contract
            AND topic1 == unhex('000000000000000000000000C2132D05D31C914A87C6611C10748AEB04B58E8F') -- usdt
), supply AS (
    SELECT 'supply' AS type,
        signed_at,
        tx_hash,
        sender,
        to_u256_raw(data1)/pow(10, 6) AS amount
    FROM aave_usdt
    WHERE topic0 == unhex('2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61') -- supply
        AND data1 IS NOT NULL
), withdraw AS (
    SELECT 'withdraw' AS type,
        signed_at,
        tx_hash,
        sender,
        to_u256_raw(data0)/pow(10, 6) AS amount
    FROM aave_usdt
    WHERE topic0 == unhex('3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7') -- withdraw
        AND data0 IS NOT NULL
), borrow AS (
    SELECT 'borrow' AS type,
        signed_at,
        tx_hash,
        sender,
        to_u256_raw(data1)/pow(10, 6) AS amount
    FROM aave_usdt
    WHERE topic0 == unhex('b3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0') -- borrow
        AND data1 IS NOT NULL
), repay AS (
    SELECT 'repay' AS type,
        signed_at,
        tx_hash,
        sender,
        to_u256_raw(data0)/pow(10, 6) AS amount
    FROM aave_usdt
    WHERE topic0 == unhex('a534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051') -- repay
        AND data0 IS NOT NULL
), supply_period AS (
    SELECT 'supply' AS type,
        [signed_at:aggregation] AS date,
        sum(amount) AS amount
    FROM supply
    GROUP BY date
), withdraw_period AS (
    SELECT 'withdraw' AS type,
        [signed_at:aggregation] AS date,
        -sum(amount) AS amount
    FROM withdraw
    GROUP BY date
), borrow_period AS (
    SELECT 'borrow' AS type,
        [signed_at:aggregation] AS date,
        -sum(amount) AS amount
    FROM borrow
    GROUP BY date
), repay_period AS (
    SELECT 'repay' AS type,
        [signed_at:aggregation] AS date,
        sum(amount) AS amount
    FROM repay
    GROUP BY date
), inflow_period AS (
    SELECT 'sum_inflow' AS type,
        su.date AS date,
        su.amount+re.amount AS amount
    FROM supply_period su
    FULL OUTER JOIN repay_period re ON re.date = su.date
), outflow_period AS (
    SELECT 'sum_outflow' AS type,
        wi.date AS date,
        wi.amount+bo.amount AS amount
    FROM withdraw_period wi
    FULL OUTER JOIN borrow_period bo ON bo.date = wi.date
), total_period AS (
    SELECT 'net_total' AS type,
        i.date AS date,
        i.amount+o.amount AS amount
    FROM inflow_period i
    FULL OUTER JOIN outflow_period o ON o.date = i.date
)
SELECT * FROM (
    SELECT * FROM supply_period
    UNION ALL
    SELECT * FROM withdraw_period
    UNION ALL
    SELECT * FROM borrow_period
    UNION ALL
    SELECT * FROM repay_period
    UNION ALL
    SELECT * FROM inflow_period
    UNION ALL
    SELECT * FROM outflow_period
    UNION ALL
    SELECT * FROM total_period
)
WHERE amount IS NOT NULL
ORDER BY date