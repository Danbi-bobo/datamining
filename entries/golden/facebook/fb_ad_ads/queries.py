get_all_ads_query = '''
    SELECT
        b.team, a.ad_id
    FROM
        lollibooks_data_golden.fb_ad_insights a
        LEFT JOIN lollibooks_data_golden.fb_ad_accounts b ON a.account_id = b.id
        LEFT JOIN lollibooks_data_golden.fb_ad_ads c ON a.ad_id = c.ad_id
    WHERE
        c.ad_id IS NULL
    UNION
    SELECT
        team, ad_id
    FROM
        lollibooks_data_golden.fb_ad_insights
    WHERE 
        date >= CURRENT_DATE
        AND spend > 0
'''