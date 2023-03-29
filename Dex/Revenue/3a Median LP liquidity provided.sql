SELECT
    [signed_at:aggregation] AS date,
    'WBTC<>WETH' AS pair_ticker,
	quantile(amount1_unscaled/power(10, prices1.num_decimals)*prices1.price_in_usd)*2 as token1_added
FROM reports.dex dex
LEFT JOIN (
    SELECT dt, contract_address, price_in_usd, num_decimals
    FROM reports.token_prices 
    WHERE chain_id == 42161
		) prices1
			ON hex(dex.token1_address) = upper(prices1.contract_address)
				AND date_trunc('day', dex.signed_at) = prices1.dt
WHERE chain_id == 42161
    AND [signed_at:daterange]
    AND pair_address == unhex('515e252b2b5c22b4b2b6Df66c2eBeeA871AA4d69')
	AND event == 'add_liquidity'
GROUP BY date