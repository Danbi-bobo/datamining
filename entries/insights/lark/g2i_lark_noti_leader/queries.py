detail_market = '''
WITH raw_orders AS (
	SELECT DISTINCT
		order_id
		, CASE 
			WHEN COALESCE(a.order_currency, b.currency) IN ('USD', 'KHR') THEN (cod + transfer_money) * c.exchange_rate_to_VND / 100 + fee_marketplace
			ELSE (cod + transfer_money) * c.exchange_rate_to_VND + fee_marketplace
		END AS sales
		, DATE(DATE_ADD(inserted_at, INTERVAL 7 HOUR)) AS inserted_date
		, CASE
			WHEN status IN (0, 6, 7) THEN 0
			ELSE 1
		END AS status
        , marketer_id
        , b.region
	FROM
		lollibooks_data_golden.pos_orders a
		LEFT JOIN lollibooks_data_golden.lark_mapping_pos_shops b ON a.shop_id = b.shop_id
		LEFT JOIN lollibooks_data_golden.lark_exchange_rate c 
			ON COALESCE(a.order_currency, b.currency) = c.currency 
			AND DATE(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) >= c.from_at
			AND DATE(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) < c.until_at
	WHERE
		NOT JSON_CONTAINS(tags_id, 64)
		AND DATE_ADD(inserted_at, INTERVAL 7 HOUR) >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
),
golden_orders AS (
	SELECT
		region
		, inserted_date AS date
		, sum(sales * status) AS total_sales
		, CASE
			WHEN count(*) = 0 THEN 0
        	ELSE sum(status) / count(*) 
        END AS closed_rate
	FROM 
		raw_orders a
	GROUP BY
		region
		, a.inserted_date
),
raw_fb_insights AS (
	SELECT
		c.region
		, a.date
		, SUM(
			CASE
				WHEN b.currency = 'USD' THEN a.spend * d.exchange_rate_to_VND
				ELSE a.spend * d.exchange_rate_to_VND
			END
		) AS spend
	FROM
		lollibooks_data_golden.fb_ad_insights a
        LEFT JOIN lollibooks_data_golden.fb_ad_accounts b ON a.account_id = b.id
        LEFT JOIN lollibooks_data_golden.lark_mapping_team_region c ON b.team = c.team
        LEFT JOIN lollibooks_data_golden.lark_exchange_rate d 
			ON b.currency = d.currency
			AND a.date >= d.from_at
			AND a.date < d.until_at
	WHERE
		a.date >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
	GROUP BY
		c.region
		, a.date
),
gg_insights AS (
	SELECT
		c.region
		, a.date
		, sum(a.spend) AS spend
	FROM
		lollibooks_data_golden.gg_ad_insights a
		LEFT JOIN lollibooks_data_golden.gg_ad_accounts b ON a.account_id = b.id
		LEFT JOIN lollibooks_data_golden.lark_mapping_team_region c ON b.team = c.team
	GROUP BY
		c.region
		, a.`date`
)
SELECT
	a.region as `Thị trường`
	, a.date AS `Ngày`
	, b.total_sales AS `Doanh số`
	, b.closed_rate AS `Tỉ lệ chốt`
	, IFNULL(a.spend, 0) + IFNULL(c.spend, 0) AS `Chi tiêu Ads`
    , CASE 
		WHEN IFNULL(b.total_sales, 0) = 0 THEN 1
		ELSE (IFNULL(a.spend, 0) + IFNULL(c.spend, 0)) / b.total_sales
	END AS `%Ads`
FROM 
	raw_fb_insights a
	LEFT JOIN golden_orders b ON a.date = b.date AND a.region = b.region
	LEFT JOIN gg_insights c ON a.date = c.date AND a.region = c.region
'''

detail_team = '''
WITH mkt_source AS (
	SELECT
		DATE(DATE_ADD(from_at, INTERVAL 7 HOUR)) AS from_at
		, DATE(DATE_ADD(until_at, INTERVAL 7 HOUR)) AS until_at
		, order_source_id
		, order_source_name
		, mkt_employee
		, product
		, team
	FROM
		lollibooks_data_golden.lark_mkt_sources
),
raw_orders AS (
	SELECT DISTINCT
		order_id
		, CASE 
			WHEN COALESCE(a.order_currency, b.currency) IN ('USD', 'KHR') THEN (cod + transfer_money) * c.exchange_rate_to_VND / 100  + fee_marketplace
			ELSE (cod + transfer_money) * c.exchange_rate_to_VND + fee_marketplace
		END AS sales
		, CASE
			WHEN account IS NULL AND order_sources IS NOT NULL THEN order_sources
			WHEN account = 'spo_1313678411' THEN '860009663'
			WHEN account = 'spo_1310490782' THEN '1720009436'
			WHEN d.id IS NOT NULL THEN d.id
			ELSE account
		END AS account
		, order_sources
		, order_sources_name
		, p_utm_source
		, DATE(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) AS inserted_date
		, CASE
			WHEN status IN (0, 6, 7) THEN 0
			ELSE 1
		END AS status
        , marketer_id
        , b.region
	FROM
		lollibooks_data_golden.pos_orders a
		LEFT JOIN lollibooks_data_golden.lark_mapping_pos_shops b ON a.shop_id = b.shop_id
		LEFT JOIN lollibooks_data_golden.lark_exchange_rate c 
			ON COALESCE(a.order_currency, b.currency) = c.currency 
			AND DATE(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) >= c.from_at
			AND DATE(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) < c.until_at
		LEFT JOIN lollibooks_data_golden.pos_order_sources d 
			ON a.account = d.custom_id 
			AND a.shop_id = d.shop_id
	WHERE
		NOT JSON_CONTAINS(tags_id, 64)
		AND DATE_ADD(a.inserted_at, INTERVAL 7 HOUR) >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
),
golden_orders AS (
	SELECT
		inserted_date AS date
		, COALESCE(b.team, c.team, d.team) AS team
		, sum(sales * status) AS total_sales
		, CASE
			WHEN count(*) = 0 THEN 0
        	ELSE sum(status) / count(*) 
        END AS closed_rate
        , a.region
	FROM 
		raw_orders a
		LEFT JOIN lollibooks_data_golden.lark_render_utm b ON a.p_utm_source = b.utm_source
		LEFT JOIN mkt_source c ON a.account = c.order_source_id AND a.inserted_date < c.until_at AND a.inserted_date >= c.from_at
        LEFT JOIN lollibooks_data_golden.lark_mapping_marketer d ON a.marketer_id = d.marketer_id
	GROUP BY
		a.region
		, a.inserted_date
		, COALESCE(b.team, c.team, d.team)
    HAVING
		sum(status) / count(*) > 0
),
raw_fb_insights AS (
	SELECT
		c.region
		, a.date
		, b.team
		, SUM(
			CASE
				WHEN b.currency = 'USD' THEN a.spend * d.exchange_rate_to_VND
				ELSE a.spend * d.exchange_rate_to_VND
			END
		) AS spend
	FROM
		lollibooks_data_golden.fb_ad_insights a
		LEFT JOIN lollibooks_data_golden.fb_ad_accounts b ON a.account_id = b.id
		LEFT JOIN lollibooks_data_golden.lark_mapping_team_region c ON b.team = c.team
		LEFT JOIN lollibooks_data_golden.lark_exchange_rate d 
			ON b.currency = d.currency
			AND a.date >= d.from_at
			AND a.date < d.until_at
	WHERE
		date >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
	GROUP BY
		c.region
		, a.date
		, b.team
),
gg_insights AS (
	SELECT
		c.region
		, a.date
		, b.team
		, sum(a.spend) AS spend
	FROM
		lollibooks_data_golden.gg_ad_insights a
		LEFT JOIN lollibooks_data_golden.gg_ad_accounts b ON a.account_id = b.id
		LEFT JOIN lollibooks_data_golden.lark_mapping_team_region c ON b.team = c.team
	GROUP BY
		c.region
		, a.`date`
		, b.team
)
SELECT
	a.region as `Thị trường`
	, a.date AS `Ngày`
	, a.team AS `Team`
	, IFNULL(a.total_sales, 0) AS `Doanh số`
	, IFNULL(a.closed_rate, 0) AS `Tỉ lệ chốt`
	, IFNULL(b.spend, 0) + IFNULL(c.spend, 0) AS `Chi tiêu Ads`
    , CASE 
		WHEN IFNULL(a.total_sales, 0) = 0 THEN 1
		ELSE (IFNULL(b.spend, 0) + IFNULL(c.spend, 0)) / a.total_sales
	END AS `%Ads`
FROM 
	golden_orders a
	LEFT JOIN raw_fb_insights b 
    	ON a.date = b.date 
        AND a.region = b.region
        AND a.team = b.team 
	LEFT JOIN gg_insights c 
    	ON a.date = c.date 
        AND a.region = c.region
        AND a.team = c.team 
ORDER BY
	a.date DESC
	, a.team
'''

overview = '''
WITH raw_orders AS (
	SELECT DISTINCT
		order_id
		, CASE 
			WHEN COALESCE(a.order_currency, b.currency) IN ('USD', 'KHR') THEN (cod + transfer_money) * c.exchange_rate_to_VND / 100 + fee_marketplace
			ELSE (cod + transfer_money) * c.exchange_rate_to_VND + fee_marketplace
		END AS sales
		, DATE(DATE_ADD(inserted_at, INTERVAL 7 HOUR)) AS inserted_date
		, CASE
			WHEN status IN (0, 6, 7) THEN 0
			ELSE 1
		END AS status
	FROM
		lollibooks_data_golden.pos_orders a
		LEFT JOIN lollibooks_data_golden.lark_mapping_pos_shops b ON a.shop_id = b.shop_id
		LEFT JOIN lollibooks_data_golden.lark_exchange_rate c 
			ON COALESCE(a.order_currency, b.currency) = c.currency 
			AND DATE(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) >= c.from_at
			AND DATE(DATE_ADD(a.inserted_at, INTERVAL 7 HOUR)) < c.until_at
	WHERE
		NOT JSON_CONTAINS(tags_id, 64)
		AND DATE_ADD(inserted_at, INTERVAL 7 HOUR) >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
),
golden_orders AS (
	SELECT
		inserted_date AS date
		, sum(sales * status) AS total_sales
		, CASE
			WHEN count(*) = 0 THEN 0
        	ELSE sum(status) / count(*) 
        END AS closed_rate
	FROM 
		raw_orders a
	GROUP BY
		a.inserted_date
),
raw_fb_insights AS (
	SELECT
		a.date
		, SUM(
			CASE
				WHEN b.currency = 'USD' THEN a.spend * d.exchange_rate_to_VND
				ELSE a.spend * d.exchange_rate_to_VND
			END
		) AS spend
	FROM
		lollibooks_data_golden.fb_ad_insights a
        LEFT JOIN lollibooks_data_golden.fb_ad_accounts b ON a.account_id = b.id
        LEFT JOIN lollibooks_data_golden.lark_mapping_team_region c ON b.team = c.team
        LEFT JOIN lollibooks_data_golden.lark_exchange_rate d 
			ON b.currency = d.currency
			AND a.date >= d.from_at
			AND a.date < d.until_at
	WHERE
		a.date >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
	GROUP BY
		a.date
),
gg_insights AS (
	SELECT
		a.date
		, sum(a.spend) AS spend
	FROM
		lollibooks_data_golden.gg_ad_insights a
		LEFT JOIN lollibooks_data_golden.gg_ad_accounts b ON a.account_id = b.id
		LEFT JOIN lollibooks_data_golden.lark_mapping_team_region c ON b.team = c.team
	GROUP BY
		a.`date`
)
SELECT
	a.date AS `Ngày`
	, b.total_sales AS `Doanh số`
	, b.closed_rate AS `Tỉ lệ chốt`
	, IFNULL(a.spend, 0) + IFNULL(c.spend, 0) AS `Chi tiêu Ads`
    , CASE 
		WHEN IFNULL(b.total_sales, 0) = 0 THEN 1
		ELSE (IFNULL(a.spend, 0) + IFNULL(c.spend, 0)) / b.total_sales
	END AS `%Ads`
FROM 
	raw_fb_insights a
	LEFT JOIN golden_orders b ON a.date = b.date
	LEFT JOIN gg_insights c ON a.date = c.date
'''