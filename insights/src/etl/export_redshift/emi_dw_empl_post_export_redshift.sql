CREATE OR REPLACE PROCEDURE [OWNERID].emi_dw_empl_post_export()
	LANGUAGE plpgsql
AS $$

BEGIN 
	
EXECUTE 'DROP TABLE IF EXISTS [OWNERID].TMP_T_FACT_EMPL_INS_LOG';
EXECUTE 'CREATE TABLE IF NOT EXISTS [OWNERID].TMP_T_FACT_EMPL_INS_LOG(clnt_obj_id VARCHAR (100), row_count INTEGER)';


EXECUTE 'DELETE  FROM [OWNERID].t_fact_empl_ins WHERE CLNT_OBJ_ID IN (SELECT DISTINCT CLNT_OBJ_ID FROM [OWNERID].t_fact_empl_ins_stg)';

EXECUTE 'INSERT INTO [OWNERID].t_fact_empl_ins
            SELECT 
             *
             FROM [OWNERID].t_fact_empl_ins_stg';

EXECUTE 'INSERT INTO [OWNERID].TMP_T_FACT_EMPL_INS_LOG 
          SELECT CLNT_OBJ_ID, COUNT(*) from 
              [OWNERID].t_fact_empl_ins_stg
               group by clnt_obj_id';

EXECUTE 'DELETE  FROM [OWNERID].t_fact_empl_ins WHERE CLNT_OBJ_ID IN (SELECT DISTINCT clnt_obj_id FROM T_CLNT WHERE clnt_live_clsfn = ''T'')';

COMMIT;
EXCEPTION WHEN OTHERS THEN 
ROLLBACK;
END;
$$
;



CALL [OWNERID].emi_dw_empl_post_export()