BEGIN
               
    PKG_DEPLOY_UTIL.DROPTABLE('DROP TABLE TMP_T_FACT_EMPL_INS_LOG');
 
    DBMS_UTILITY.EXEC_DDL_STATEMENT ('CREATE TABLE TMP_T_FACT_EMPL_INS_LOG (clnt_obj_id VARCHAR2 (100), row_count NUMBER(10))');
    -- Loop over clients with intermediate commits
    FOR I IN (SELECT DISTINCT clnt_obj_id FROM T_FACT_EMPL_INS_STG  )
    LOOP
                    EXECUTE IMMEDIATE 'INSERT INTO TMP_T_FACT_EMPL_INS_LOG VALUES (''' || I.clnt_obj_id || ''', NULL)';
            COMMIT;
                                               
            PKG_PARTITION_SWAP.P_TRUNCATE_PARTITION('T_FACT_EMPL_INS','P'||I.CLNT_OBJ_ID||'');
   
            EXECUTE IMMEDIATE 'INSERT INTO T_FACT_EMPL_INS
                SELECT /*+ parallel(x,4) */
                    *
                FROM T_FACT_EMPL_INS_STG
                WHERE clnt_obj_id = ''' || I.clnt_obj_id || '''';
                                                               
            EXECUTE IMMEDIATE 'UPDATE TMP_T_FACT_EMPL_INS_LOG SET row_count = ' || SQL%ROWCOUNT || ' WHERE clnt_obj_id = ''' || I.clnt_obj_id || '''';
                                               
            COMMIT;
 
            -- We will not handle exceptions and let the main process fail.
       
    END LOOP;

    --Loop over terminated clients and remove their data
    FOR I IN (SELECT DISTINCT clnt_obj_id FROM T_CLNT WHERE clnt_live_ind = 'T')
    LOOP
        PKG_PARTITION_SWAP.P_TRUNCATE_PARTITION ('T_FACT_EMPL_INS','P' || I.CLNT_OBJ_ID || '');
    END LOOP;
 
END;