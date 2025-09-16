select 
    target_key,
    connection_id,
    connection_password,
    product_name,
    product_version,
    owner_id,
    dsn,
    client_identifier,
    session_setup_call,
    adpr_extension
    from
    (SELECT b.target_key,
        b.connection_id,
        b.connection_password,
        c.product_name,
        c.product_version,
        b.owner_id,
        a.dsn,
        b.client_identifier,
        b.session_setup_call,
        b.adpr_extension,
        row_number() over(order by a.dsn) as dsn_order
    FROM instances a, targets b, products c 
    WHERE a.instance_key = b.instance_key 
    AND b.product_key = c.product_key 
    AND c.product_name = '&ENV_STAR_SCHEMA_NEXTGEN' 
    AND b.active_flag IN ('t', 'i') 
    and b.OWNER_ID in ('wfncore') 
    )
where dsn_order = 3