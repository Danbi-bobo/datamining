ad_accounts_query = '''
    SELECT 
        customer_client.id AS id,
        customer_client.descriptive_name AS name,
        customer_client.currency_code AS currency,
        customer_client.time_zone AS time_zone,
        customer_client.manager AS is_manager,
        customer_client.test_account AS is_test_account,
        customer_client.status AS status,
        customer_client.level AS level,
        customer_client.client_customer AS client_customer
    FROM customer_client
'''