new_ad_accounts_query = '''
    SELECT
        id AS `ID TKQC`
        , name AS `Tên TKQC`
        , team AS Team
        , CASE
            WHEN status_name is null then "Mất quyền truy cập"
            else status_name 
        END AS `Trạng thái mới`
        , user_tasks AS `Quyền của Via với TKQC`
        , balance AS `Dư nợ`
    FROM
        lollibooks_data_golden.fb_ad_accounts
'''