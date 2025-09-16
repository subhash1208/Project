SELECT *
FROM ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_tm_tf_mngr
WHERE yr_cd in (year(current_date)+1,year(current_date),year(current_date)-1,year(current_date)-2)
-- and environment= '${hiveconf:environment}'