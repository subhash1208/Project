SELECT 
    b.target_key,
    b.connection_id,
    b.connection_password,
    c.product_name,
    c.product_version,
    b.owner_id,
    a.dsn,
    b.client_identifier,
    b.session_setup_call,
    b.adpr_extension 
FROM instances a, targets b, products c 
WHERE a.instance_key = b.instance_key
AND b.product_key = c.product_key
AND c.product_name = '&ENV_STAR_SCHEMA'
AND b.active_flag IN ('t', 'i')
AND (b.owner_id in  ('PROD_ADP_THE') or c.product_name in ('DIT_STAR_SCHEMA','FIT_STAR_SCHEMA'))