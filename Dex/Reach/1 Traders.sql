SELECT
    [signed_at:aggregation] AS date,
    'WBTC<>WETH' AS pair_ticker,
    uniq(sender) AS traders
FROM reports.dex
WHERE chain_id == 42161
    AND [signed_at:daterange]
    AND pair_address == unhex('515e252b2b5c22b4b2b6Df66c2eBeeA871AA4d69')
	AND event == 'swap'
GROUP BY date

UNION ALL

SELECT
    [signed_at:aggregation] AS date,
    'WETH<>USDT' AS pair_ticker,
    uniq(sender) AS traders
FROM reports.dex
WHERE chain_id == 42161
    AND [signed_at:daterange]
    AND pair_address == unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
	AND event == 'swap'
GROUP BY date