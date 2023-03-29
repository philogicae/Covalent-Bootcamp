SELECT
    [signed_at:aggregation] AS date,
    'WETH<>USDT' AS pair_ticker,
	quantile(amount0_unscaled/power(10, prices0.num_decimals)*prices0.price_in_usd)*2 as token0_added
FROM reports.dex dex
LEFT JOIN (
    SELECT dt, contract_address, price_in_usd, num_decimals
    FROM reports.token_prices 
    WHERE chain_id == 42161
		) prices0
			ON hex(dex.token0_address) = upper(prices0.contract_address)
				AND date_trunc('day', dex.signed_at) = prices0.dt
WHERE chain_id == 42161
    AND [signed_at:daterange]
    AND pair_address == unhex('CB0E5bFa72bBb4d16AB5aA0c60601c438F04b4ad')
	AND event == 'add_liquidity'
GROUP BY date