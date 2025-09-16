select
   index as _id,
   index,
   clnt_obj_id,
   pers_obj_id,
   gndr_dsc,
   martl_stus_dsc,
   y,
   y_hat,
   quarter,
   source_system,
   l2_code
from
   ${hiveconf:__RO_GREEN_MAIN_DB__}.top_sampled