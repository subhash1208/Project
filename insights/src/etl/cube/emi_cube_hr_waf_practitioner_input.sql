SELECT *
FROM ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_hr_waf
WHERE yr_cd in (year(current_date)+1,year(current_date),year(current_date)-1,year(current_date)-2)
--DISTRIBUTE BY environment,yr_cd
--and environment= '${hiveconf:environment}'