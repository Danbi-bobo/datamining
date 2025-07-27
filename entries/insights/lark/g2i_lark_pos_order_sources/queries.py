list_order_sources = '''      
WITH pages AS (
	SELECT DISTINCT
		actor_id
	FROM
		lollibooks_data_golden.fb_ad_adcreatives
)
SELECT
    a.shop_id AS `Shop ID`,
    CONCAT_WS(
        ' - '
        , b.name
        , CASE
            WHEN a.id = '547799061755296' THEN 'Hoài Thu (Loan)'
            ELSE a.name 
        END
    ) AS `Tên nguồn chuyển đổi`,
    a.id AS `ID nguồn`,
    a.parent_id,
    a.link_source_id,
    b.name AS `Loại nguồn`,
    CASE
    	WHEN a.updated_flag <> -1 THEN 'Bình thường'
    	ELSE 'Đã gỡ khởi POS'
    END AS `Trạng thái`    
FROM
    lollibooks_data_golden.pos_order_sources a
    LEFT JOIN lollibooks_data_golden.pos_order_sources b ON a.parent_id = b.id and a.shop_id = b.shop_id
UNION ALL
SELECT
	'5508836' AS `Shop ID`,
    NULL AS `Tên nguồn chuyển đổi`,
    actor_id AS `ID nguồn`,
    '-1' AS parent_id,
    NULL AS link_source_id,
    'Facebook' AS `Loại nguồn`,
    'Page đã tiêu tiền nhưng chưa link vào POS' AS `Trạng thái`
FROM
	pages
WHERE
	actor_id NOT IN (SELECT DISTINCT id FROM lollibooks_data_golden.pos_order_sources)
'''