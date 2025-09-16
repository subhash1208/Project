
CREATE TABLE "ADPI_DXR"."SPARK_COUNT_CATALOG_TABLE"
(	"SCHEMA_NAME" VARCHAR2(100 BYTE),
"TABLE_NAME" VARCHAR2(200 BYTE),
"SKIP_FILTERS" NUMBER DEFAULT 0
) ;

--------------------------------------------------------
--  Constraints for Table SPARK_COUNT_CATALOG_TABLE
--------------------------------------------------------

ALTER TABLE "ADPI_DXR"."SPARK_COUNT_CATALOG_TABLE" ADD CONSTRAINT "SPARK_COUNT_CATALOG_TABLE_PK" PRIMARY KEY ("SCHEMA_NAME", "TABLE_NAME");
ALTER TABLE "ADPI_DXR"."SPARK_COUNT_CATALOG_TABLE" MODIFY ("TABLE_NAME" NOT NULL ENABLE);
ALTER TABLE "ADPI_DXR"."SPARK_COUNT_CATALOG_TABLE" MODIFY ("SKIP_FILTERS" NOT NULL ENABLE);
ALTER TABLE "ADPI_DXR"."SPARK_COUNT_CATALOG_TABLE" MODIFY ("SCHEMA_NAME" NOT NULL ENABLE);

REM INSERTING into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE
SET DEFINE OFF;
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','ap_control_ooid_map',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','ap_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','ap_monthly_company_ein',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_base_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_core_master',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_dw_prfl_setting',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_ein_info_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_master',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_ownership_type_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_paycodes_classified',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_prmry_attr_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','client_prmry_attr_qtrly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','dwh_client_jobs',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','employee_base_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','janzz_input_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','janzz_input_monthly_hash_usa',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','janzz_output_master',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','janzz_result_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_client',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_company',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_department',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_earning_type',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_earnings',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_education',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_establishment',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_exchange_rates',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_geodiff',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_job',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_licenses_certifications',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_parent_paygroup',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_paygroup',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_person',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_person_job_dates',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_salary_plan',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_skill',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_work_assignment',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_work_event',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_work_event_type',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_work_location',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','nas_work_person',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','paycodes_classified',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','time_classified_paycodes',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','time_transaction',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','time_transaction_v2',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tmp_janzz_input_uniq_usa',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tmp_janzz_output_uniq_usa',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tmp_paycodes_raw_input',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_client',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_company',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_department',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_establishment',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_job',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_paygroup',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_person',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_person_job_dates',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_work_assignment',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_work_event',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_work_event_type',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','tts_work_location',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','wfn_pay_grades',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','wfn_person',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','wfn_work_assignment',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('base','wfn_work_state',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','benchmark_core_annual',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','benchmark_core_quarterly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','benchmark_cube_annual',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','bfunc_predict_monthly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','btype_app_stg_2',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','client_level_bm_core_quarterly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','client_level_bm_cube_quarterly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','employee_monthly',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','employee_reporting_chain',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','jlevel_predict_monthly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','jlevel_valid_monthly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','mnc_optout',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','p_stats',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','span_ctrl',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','span_ctrl_core',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','span_ctrl_cube',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','span_ctrl_cube_quarterly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','span_ctrl_cube_raw',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_annl_comp_temp',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_annl_data',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_annl_data_raw',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_clnt_lvl_mnthly_data',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_clnt_lvl_qtrly_data',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_mtrc_rcmndns',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_qtrly_data',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_qtrly_span_of_cntl',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_rpt_rcmndns',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_tm_mnthly_data',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_bnchmrk_tm_qtrly_data',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_clnt_inds',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','t_dim_day',1);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','time_bm_core_monthly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','time_bm_core_quarterly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','time_bm_cube_monthly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','time_bm_cube_quarterly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','time_clnt_bm_core_monthly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','time_clnt_bm_core_quarterly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','time_clnt_bm_cube_monthly',0);
Insert into ADPI_DXR.SPARK_COUNT_CATALOG_TABLE (SCHEMA_NAME,TABLE_NAME,SKIP_FILTERS) values ('main','time_clnt_bm_cube_quarterly',0);
