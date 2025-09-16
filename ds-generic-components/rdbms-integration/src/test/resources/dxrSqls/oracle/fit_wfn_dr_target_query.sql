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
        WHERE
            a.instance_key = b.instance_key
            AND b.product_key = c.product_key
            AND b.active_flag in ('t','i')
            AND c.product_name = 'FIT_WFNDR'
            AND rownum<100