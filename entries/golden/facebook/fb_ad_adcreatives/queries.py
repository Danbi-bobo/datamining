get_all_creative = '''
    SELECT DISTINCT
	    c.team, b.creative_id
    FROM
        lollibooks_data_golden.fb_ad_adcreatives a
        RIGHT JOIN lollibooks_data_golden.fb_ad_ads b ON a.creative_id = b.creative_id
        LEFT JOIN lollibooks_data_golden.fb_ad_accounts c ON b.account_id = c.id
    WHERE
        a.creative_id IS NULL
    UNION
    SELECT DISTINCT
        a.team, b.creative_id
    FROM
        lollibooks_data_golden.fb_ad_insights a
        LEFT JOIN lollibooks_data_golden.fb_ad_ads b ON a.ad_id = b.ad_id
    WHERE 
        a.date >= CURRENT_DATE
        AND a.spend > 0
        and b.creative_id IS NOT NULL
'''

get_post_creative = '''
    WITH creatives AS (
        SELECT
            b.creative_id
        FROM
            lollibooks_data_golden.fb_ad_insights a
            LEFT JOIN lollibooks_data_golden.fb_ad_ads b ON a.ad_id = b.ad_id
        WHERE 
            a.spend > 0
            AND b.creative_id IS NOT NULL
        GROUP BY
            b.creative_id
    )
    SELECT
        a.creative_id, a.object_story_id, a.actor_id as page_id, b.page_access_token
    FROM
        lollibooks_data_golden.fb_ad_adcreatives a
        LEFT JOIN lollibooks_data_golden.fb_pages b ON a.actor_id = b.page_id
    WHERE
        a.object_story_id IS NOT NULL
        AND a.link IS NULL
        AND a.creative_id IN (SELECT * FROM creatives)
        AND b.page_access_token IS NOT NULL
'''