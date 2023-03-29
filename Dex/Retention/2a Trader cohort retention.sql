with user_cohorts AS (
    SELECT 
        sender,
        min(date_trunc('month', signed_at)) AS cohortMonth
    FROM reports.dex
    WHERE chain_id = 42161
        AND signed_at > '2021-12-31'
        AND pair_address = unhex('515e252b2b5c22b4b2b6Df66c2eBeeA871AA4d69')
        AND event = 'swap'
    GROUP BY sender
), following_months AS (
    SELECT
        sender,
        date_diff('month', uc.cohortMonth, date_trunc('month', signed_at)) AS month_number
    FROM reports.dex d
    LEFT JOIN user_cohorts uc ON d.sender = uc.sender
    WHERE chain_id = 42161
        AND signed_at > '2021-12-31'
        AND pair_address = unhex('515e252b2b5c22b4b2b6Df66c2eBeeA871AA4d69')
        AND event = 'swap'
    GROUP BY sender, month_number
), cohort_size AS (
    SELECT
        uc.cohortMonth AS cohortMonth,
        count(*) AS num_users
    FROM user_cohorts uc
    GROUP BY cohortMonth
    ORDER BY cohortMonth
), retention_table AS (
    SELECT 
        c.cohortMonth AS cohortMonth,
        o.month_number AS month_number,
        count(*) AS num_users
    FROM following_months o
    LEFT JOIN user_cohorts c ON o.sender = c.sender
    GROUP BY cohortMonth, month_number
)

SELECT
    r.cohortMonth,
    s.num_users AS new_users,
    r.month_number,
    r.num_users / s.num_users AS retention
FROM retention_table r
LEFT JOIN cohort_size s 
	ON r.cohortMonth = s.cohortMonth
WHERE r.month_number != 0
    AND [cohortMonth:daterange]
ORDER BY r.cohortMonth, r.month_number