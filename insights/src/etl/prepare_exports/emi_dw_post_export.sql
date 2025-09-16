BEGIN
    IF '&INCREMENTAL_EXPORT' = 'false' THEN
        EXECUTE IMMEDIATE 'TRUNCATE TABLE T_FACT_INS';
    END IF;
               
    PKG_DEPLOY_UTIL.DROPTABLE('DROP TABLE TMP_T_FACT_INS_LOG');
 
    DBMS_UTILITY.EXEC_DDL_STATEMENT ('CREATE TABLE TMP_T_FACT_INS_LOG (clnt_obj_id VARCHAR2 (100), row_count NUMBER(10))');
    -- Loop over clients with intermediate commits
    FOR I IN (SELECT DISTINCT clnt_obj_id FROM T_FACT_INS_STG  )
    LOOP
                    EXECUTE IMMEDIATE 'INSERT INTO TMP_T_FACT_INS_LOG VALUES (''' || I.clnt_obj_id || ''', NULL)';
            COMMIT;
                                               
            PKG_PARTITION_SWAP.P_TRUNCATE_PARTITION('T_FACT_INS','P'||I.CLNT_OBJ_ID||'');
   
            EXECUTE IMMEDIATE 'INSERT INTO T_FACT_INS
                SELECT /*+ parallel(x,4) */
                    *
                FROM T_FACT_INS_STG
                WHERE clnt_obj_id = ''' || I.clnt_obj_id || '''';
                                                               
            EXECUTE IMMEDIATE 'UPDATE TMP_T_FACT_INS_LOG SET row_count = ' || SQL%ROWCOUNT || ' WHERE clnt_obj_id = ''' || I.clnt_obj_id || '''';
                                               
            COMMIT;
 
            -- We will not handle exceptions and let the main process fail.
       
    END LOOP;
 
END;