SELECT b.target_key, 
       b.connection_id, 
       b.connection_password, 
       c.product_name, 
       c.product_version, 
       b.owner_id, 
       a.dsn, 
       b.client_identifier, 
       b.session_setup_call, 
       b.adpr_extension 
FROM   instances a, 
       targets b, 
       products c 
WHERE  a.instance_key = b.instance_key 
       AND b.product_key = c.product_key 
       AND c.product_name = 'DATACLOUD'
       AND b.owner_id LIKE 'ADPDC_WFN%'
    --    AND ( b.owner_id LIKE 'ADPDC_WFN%'
    --           OR b.owner_id IN ( 'ADPIDMCORE10', 'ADPIDMCORE20', 'ADPIDMCORE01',
    --                              'ADPIDMCORE03' ) )
       AND b.active_flag IN ( 't', 'i' )