query = '''
WITH base_data AS (
    SELECT 
        b.team,
        b.name AS account_name,
        date,
        WEEKDAY(date) AS weekday,
        -- Xác định ngày bắt đầu kỳ
        CASE 
            WHEN WEEKDAY(date) IN (0,1) THEN DATE_SUB(date, INTERVAL WEEKDAY(date) DAY)  -- Kỳ 1: Thứ 2 của tuần này
            WHEN WEEKDAY(date) IN (2,3) THEN DATE_SUB(date, INTERVAL WEEKDAY(date) - 2 DAY)  -- Kỳ 2: Thứ 4 của tuần này
            ELSE DATE_SUB(date, INTERVAL WEEKDAY(date) - 4 DAY)  -- Kỳ 3: Thứ 6 của tuần này
        END AS period_start,
        -- Xác định số ngày của kỳ
        CASE 
            WHEN WEEKDAY(date) IN (0,1) THEN 2  -- Kỳ 1: 2 ngày (Thứ 2, Thứ 3)
            WHEN WEEKDAY(date) IN (2,3) THEN 2  -- Kỳ 2: 2 ngày (Thứ 4, Thứ 5)
            ELSE 3  -- Kỳ 3: 3 ngày (Thứ 6, Thứ 7, Chủ Nhật)
        END AS period_days,
        spend
    FROM lollibooks_data_golden.fb_ad_insights a
    LEFT JOIN lollibooks_data_golden.fb_ad_accounts b ON a.account_id = b.id
    WHERE date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
),
period_data AS (
    SELECT 
        team,
        account_name,
        period_start,
        DATE_SUB(period_start, INTERVAL 3 DAY) AS spend_start_date,  -- Luôn là period_start - 3
        DATE_SUB(period_start, INTERVAL 1 DAY) AS spend_end_date,   -- Luôn là period_start - 1
        period_days
    FROM base_data
    GROUP BY team, account_name, period_start, period_days
),
spend_calculation AS (
    SELECT 
        p.team,
        p.account_name,
        p.period_start,
        p.spend_start_date,
        p.spend_end_date,
        p.period_days,
        SUM(b.spend) AS spend
    FROM period_data p
    JOIN base_data b 
        ON p.team = b.team 
        AND p.account_name = b.account_name
        AND b.date BETWEEN p.spend_start_date AND p.spend_end_date
    GROUP BY p.team, p.account_name, p.period_start, p.spend_start_date, p.spend_end_date, p.period_days
)
SELECT 
    team AS `Team`,
    account_name AS `TKQC`,
    'Facebook' AS `Kênh`,
    UNIX_TIMESTAMP(period_start) * 1000 AS `Kỳ nạp`,
    UNIX_TIMESTAMP(spend_start_date) * 1000 AS `Ngày bắt đầu tính chi tiêu`,
    UNIX_TIMESTAMP(spend_end_date) * 1000 AS `Ngày kết thúc tính chi tiêu`,
    spend AS `Tổng chi tiêu 3 ngày gần nhất`,
    period_days AS `Số ngày của kỳ`
FROM spend_calculation
ORDER BY team, period_start;
'''