get_all_adsets_query = '''
    SELECT DISTINCT
        team, adset_id
    FROM
        lollibooks_data_golden.fb_ad_insights
    WHERE 
        date >= CURRENT_DATE
        AND spend > 0
'''