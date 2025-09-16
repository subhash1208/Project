# Databricks notebook source

# COMMAND ----------

__CODESTORE_BUCKET__ = dbutils.widgets.get("__CODESTORE_BUCKET__")

# COMMAND ----------


files_to_add = ["emi_ins_hr_waf_practitioner.sql","emi_ins_hr_waf_practitioner.xml","emi_ins_tm_tf_practitioner.sql","emi_ins_tm_tf_practitioner.xml","emi_ins_tm_tf_excp_practitioner.sql","emi_ins_tm_tf_excp_practitioner.xml","emi_ins_pr_pef_practitioner.sql","emi_ins_pr_pef_practitioner.xml","emi_ins_hr_waf_manager.sql","emi_ins_hr_waf_manager.xml","emi_ins_tm_tf_edits_practitioner.sql","emi_ins_tm_tf_edits_practitioner.xml","emi_ins_pr_pef_manager.sql","emi_ins_pr_pef_manager.xml","emi_ins_tm_tf_edits_manager.sql","emi_ins_tm_tf_edits_manager.xml","emi_ins_tm_tf_excp_manager.sql","emi_ins_tm_tf_excp_manager.xml","emi_ins_tm_tf_excp_supervisor.sql","emi_ins_tm_tf_excp_supervisor.xml","emi_ins_tm_tf_supervisor.sql","emi_ins_tm_tf_supervisor.xml","emi_ins_tm_tf_edits_supervisor.sql","emi_ins_tm_tf_edits_supervisor.xml","emi_ins_tm_tf_manager.sql","emi_ins_tm_tf_manager.xml","emi_ins_hr_waf_practitioner.sql","emi_ins_intl_bm_hr_waf_practitioner.xml","emi_ins_bm_hr_waf.sql","emi_ins_tm_tf_edits_practitioner.sql","emi_ins_intl_bm_tm_tf_edits_practitioner.xml","emi_ins_bm_tm_tf_edits.sql","emi_ins_hr_waf_manager.sql","emi_ins_intl_bm_hr_waf_manager.xml","emi_ins_bm_hr_waf.sql","emi_ins_pr_pef_practitioner.sql","emi_ins_intl_bm_pr_pef_practitioner.xml","emi_ins_bm_pr_pef.sql","emi_ins_tm_tf_edits_manager.sql","emi_ins_intl_bm_tm_tf_edits_manager.xml","emi_ins_bm_tm_tf_edits.sql","emi_ins_pr_pef_manager.sql","emi_ins_intl_bm_pr_pef_manager.xml","emi_ins_bm_pr_pef.sql","emi_ins_tm_tf_edits_supervisor.sql","emi_ins_intl_bm_tm_tf_edits_supervisor.xml","emi_ins_bm_tm_tf_edits.sql","emi_ins_tm_tf_manager.sql","emi_ins_intl_bm_tm_tf_manager.xml","emi_ins_bm_tm_tf.sql","emi_ins_tm_tf_practitioner.sql","emi_ins_intl_bm_tm_tf_practitioner.xml","emi_ins_bm_tm_tf.sql","emi_ins_tm_tf_supervisor.sql","emi_ins_intl_bm_tm_tf_supervisor.xml","emi_ins_bm_tm_tf.sql"]

# COMMAND ----------

current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
current_path_split = current_path.split('/')
for file in files_to_add:
  file_name = current_path_split[:-1]
  file_name.append(file)
  source_path = '/'.join(file_name)
  source_path = "File:/Workspace"+source_path
  destination_path = f"s3://{__CODESTORE_BUCKET__}/code/insights/src/etl/insight/{file}"
  print(source_path, destination_path)
  dbutils.fs.cp(source_path, destination_path)

# COMMAND ----------

