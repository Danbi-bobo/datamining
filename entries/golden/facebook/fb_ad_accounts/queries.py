get_ad_accounts = '''
    SELECT
        id, account_status
    FROM
        lollibooks_data_golden.fb_ad_accounts
    WHERE
        account_status <> -1
'''