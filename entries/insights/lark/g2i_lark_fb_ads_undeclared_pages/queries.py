undeclared_page_ads = f'''      
    WITH declared_id AS (
        SELECT
            DISTINCT order_source_id AS id
        FROM
            lollibooks_data_golden.lark_mkt_sources
        WHERE
            DATE_ADD(from_at, INTERVAL 7 HOUR) <= CURRENT_DATE
            AND DATE_ADD(until_at, INTERVAL 7 HOUR) >= CURRENT_DATE
    ),
    pos_id AS (
        SELECT
            DISTINCT id
        FROM
            lollibooks_data_golden.pos_order_sources
        WHERE
            updated_flag <> -1
    )
    SELECT DISTINCT
        b.team
        , b.name as ad_account
        , a.campaign_name
        , a.adset_name
        , a.ad_name
        , a.ad_id
        , c.actor_id as page_id
        , CASE
            WHEN e.id IS NULL THEN 'Page chưa tích hợp vào POS'
            WHEN d.id IS NULL THEN 'Page chưa khai báo nhân sự phụ trách'
        END AS reason
    FROM
        lollibooks_data_golden.fb_ad_insights a
        LEFT JOIN lollibooks_data_golden.fb_ad_accounts b ON a.account_id = b.id
        LEFT JOIN lollibooks_data_golden.fb_ad_ads c ON a.ad_id = c.ad_id
        LEFT JOIN declared_id d ON c.actor_id = d.id
        LEFT JOIN pos_id e ON c.actor_id = e.id
    WHERE
        c.actor_id NOT IN ('114569841736052')
        AND (
            e.id IS NULL
            OR d.id IS NULL
        )
'''