query = '''
SELECT
	a.order_source_id AS `ID`
	, a.order_source_name AS `Tên nguồn đơn hàng`
    , a.team AS `Team`
	, a.mkt_employee_id AS `Nhân sự phụ trách hiện tại`
FROM
	lollibooks_data_golden.lark_mkt_sources a
	LEFT JOIN lollibooks_data_golden.lark_users b ON a.mkt_employee_id = b.user_id
WHERE
	from_at < until_at
	AND status = 'Resigned'
	AND DATE_ADD(until_at, INTERVAL 7 HOUR) >= CURRENT_DATE
ORDER BY
	a.team
'''