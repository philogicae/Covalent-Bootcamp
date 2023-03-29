With daily_active_users_btc as (
    SELECT
        date_trunc('month', day) as date, 
        avg(active_addresses) as avg_dau
    FROM (
        SELECT
            date_trunc('day', signed_at) as day,
            uniq(sender) AS active_addresses
        FROM reports.dex
        WHERE chain_id == 42161
            AND [signed_at:daterange]
            AND pair_address == unhex('515e252b2b5c22b4b2b6Df66c2eBeeA871AA4d69')
            AND event == 'swap'
        GROUP BY day
    ) GROUP BY date
), monthly_active_users_btc as (
	SELECT 
        date_trunc('month', signed_at) as date, 
        uniq(sender) AS mau
    FROM reports.dex
    WHERE chain_id == 42161
        AND [signed_at:daterange]
        AND pair_address == unhex('515e252b2b5c22b4b2b6Df66c2eBeeA871AA4d69')
        AND event == 'swap'
	GROUP BY date
), daily_active_users_usdt as (
    SELECT
        date_trunc('month', day) as date, 
        avg(active_addresses) as avg_dau
    FROM (
        SELECT
            date_trunc('day', signed_at) as day,
            uniq(sender) AS active_addresses
        FROM reports.dex
        WHERE chain_id == 42161
            AND [signed_at:daterange]
            AND pair_address == unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
            AND event == 'swap'
        GROUP BY day
    ) GROUP BY date
), monthly_active_users_usdt as (
	SELECT 
        date_trunc('month', signed_at) as date, 
        uniq(sender) AS mau
    FROM reports.dex
    WHERE chain_id == 42161
        AND [signed_at:daterange]
        AND pair_address == unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
        AND event == 'swap'
	GROUP BY date
)

SELECT
    daily.date as date,
    'WBTC<>WETH' AS pair_ticker,
    (daily.avg_dau/monthly.mau) as stickiness_ratio
FROM daily_active_users_btc daily
    LEFT JOIN monthly_active_users_btc monthly
  	    ON daily.date = monthly.date
ORDER BY date

UNION ALL

SELECT
    daily.date as date,
    'WETH<>USDT' AS pair_ticker,
    (daily.avg_dau/monthly.mau) as stickiness_ratio
FROM daily_active_users_usdt daily
    LEFT JOIN monthly_active_users_usdt monthly
  	    ON daily.date = monthly.date
ORDER BY date