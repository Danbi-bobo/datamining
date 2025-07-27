insights_query = '''
    SELECT 
        campaign.id AS campaign_id, 
        campaign.name AS campaign_name, 
        ad_group.id AS adset_id, 
        ad_group.name AS adset_name, 
        ad_group_ad.ad.id AS ad_id, 
        ad_group_ad.ad.name AS ad_name,
        ad_group_ad.ad.final_urls AS final_urls,
        ad_group_ad.ad.type AS ad_type,
        metrics.cost_micros as spend,
        metrics.clicks as clicks,
        metrics.conversions as conversions,
        metrics.average_cpm AS average_cpm,
        metrics.average_cpe AS average_cpe,
        metrics.average_cpc AS average_cpc,
        metrics.average_time_on_site AS average_time_on_site,
        segments.date AS date
    FROM ad_group_ad
    WHERE segments.date DURING TODAY
'''

ad_accounts_query = '''
    SELECT
        mcc_id, id
    FROM
        lollibooks_data_golden.gg_ad_accounts
    WHERE
        team <> 'Team M0'
        AND status = 'ENABLED'
        AND is_manager = 0
'''