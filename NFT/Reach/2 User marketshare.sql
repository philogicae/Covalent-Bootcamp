WITH market_users AS (
    SELECT signed_at, maker AS address
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
        AND market = 'aavegotchi'                
    UNION ALL 
    SELECT signed_at, taker AS address
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
        AND market = 'aavegotchi'
), market_users_cumul AS (
    SELECT date, sum(count()) OVER (ORDER BY date) AS users
    FROM (
        SELECT min([signed_at:aggregation]) AS date, address
        FROM market_users GROUP BY address
    ) GROUP BY date
), market_users_active AS (
    SELECT [signed_at:aggregation] AS date, uniq(address) AS users
    FROM market_users GROUP BY date
), chain_users AS (
    SELECT signed_at, maker AS address
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
    UNION ALL 
    SELECT signed_at, taker AS address
    FROM reports.nft_sales_all_chains
    WHERE chain_name = 'matic_mainnet'
), chain_users_cumul AS (
    SELECT date, sum(count()) OVER (ORDER BY date) AS users
    FROM (
        SELECT min([signed_at:aggregation]) AS date, address
        FROM chain_users GROUP BY address
    ) GROUP BY date
), chain_users_active AS (
    SELECT [signed_at:aggregation] AS date, uniq(address) AS users
    FROM chain_users GROUP BY date
)

SELECT
    cuc.date AS date,
    muc.users / cuc.users AS unique_users,
    mua.users / cua.users AS active_users
FROM chain_users_cumul cuc
LEFT JOIN market_users_cumul muc ON cuc.date = muc.date
LEFT JOIN chain_users_active cua ON cuc.date = cua.date
LEFT JOIN market_users_active mua ON cuc.date = mua.date
WHERE [date:daterange]
ORDER BY date