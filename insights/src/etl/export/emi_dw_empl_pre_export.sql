BEGIN
  pkg_deploy_util.droptable ('DROP TABLE T_FACT_EMPL_INS_STG');
  EXECUTE IMMEDIATE 'CREATE TABLE T_FACT_EMPL_INS_STG AS SELECT * FROM T_FACT_EMPL_INS WHERE 1=2';  
     --Partition creation for t_fact_ins
   p_ins_prtn_crt;
END;