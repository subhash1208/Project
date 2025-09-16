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
    b.adpr_extension,
    d.min_iid as lower_bound,
    d.max_iid as upper_bound,
    d.batch_iid as batch_number
FROM
    instances a,
    targets b,
    products c,
    run_targets_batch d
WHERE
    a.instance_key = b.instance_key
    AND   b.product_key = c.product_key
    AND   d.target_key = b.target_key
    AND   c.product_name = 'RUN'
    AND   b.active_flag IN ('t','i')
    AND   rownum <10