get_all_campaigns_query = '''
    SELECT DISTINCT
        team, campaign_id
    FROM
        lollibooks_data_golden.fb_ad_insights
    WHERE 
        date >= CURRENT_DATE
        AND spend > 0
'''