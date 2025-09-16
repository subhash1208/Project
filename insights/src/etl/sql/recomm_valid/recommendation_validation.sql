DROP VIEW IF EXISTS ${__BLUE_MAIN_DB__}.validate_recommendations;

CREATE VIEW ${__BLUE_MAIN_DB__}.validate_recommendations AS
SELECT CASE WHEN ((SUM(ip_cnt) - SUM(op_cnt)) / SUM(ip_cnt) < 0.1) THEN 'true' ELSE 'false' END AS validation
        FROM (
                SELECT 
                       count(*) as op_cnt,0 as ip_cnt FROM 
                      ${__BLUE_MAIN_DB__}.emi_recommender_dataset
                --WHERE
                --   environment='${environment}' 
                UNION ALL 
                SELECT
                         0 as op_cnt,count(*) as ip_cnt
                FROM
                (
                  SELECT
                      ins_hash_val,
                      ROW_NUMBER() OVER (PARTITION BY clnt_obj_id,ins_hash_val ORDER BY  rec_crt_ts desc) as rank
                      FROM ${__RO_BLUE_RAW_DB__}.dwh_t_fact_ins_hist
                      WHERE to_date(ins_serv_dt) >= to_date(date_sub(current_date,90)) AND delvy_chanl in ('mobile-insight','mobile-notification')
                      --AND environment='${environment}'    
                 ) sub
                 WHERE rank = 1 
               ) res;