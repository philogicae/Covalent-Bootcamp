SELECT
	avg(daily_min_usd) AS floor_price_usd
FROM (
	SELECT 
		date_trunc('day', signed_at) AS date,
		min(nullIF(nft_token_price_usd / token_count, 0)) AS daily_min_usd
	FROM reports.nft_sales_all_chains
	WHERE chain_name = 'matic_mainnet'
		AND date > now() - interval '1 month'
    	AND collection_address = unhex('86935F11C86623DEC8A25696E1C19A8659CBF95D')
	GROUP BY date
)