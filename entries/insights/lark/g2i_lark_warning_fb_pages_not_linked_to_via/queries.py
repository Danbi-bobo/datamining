pages = '''
SELECT DISTINCT
    e.team AS `Team`
    , c.actor_id AS `Page ID`
FROM
    lollibooks_data_golden.fb_ad_insights a
    LEFT JOIN lollibooks_data_golden.fb_ad_ads b ON a.ad_id = b.ad_id
    LEFT JOIN lollibooks_data_golden.fb_ad_adcreatives c ON b.creative_id = c.creative_id
    LEFT JOIN lollibooks_data_golden.fb_pages d ON c.actor_id = d.page_id
    LEFT JOIN lollibooks_data_golden.fb_ad_accounts e ON a.account_id = e.id
WHERE
    d.page_id IS NULL
ORDER BY
    e.team ASC
'''