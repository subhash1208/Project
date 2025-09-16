SELECT
    yr_cd,
    NULL AS qtr_cd,
    NULL AS mnth_cd,
    NULL AS wk_cd,
    CAST (COUNT(1) AS INT) AS sys_calendar_days
FROM
    t_dim_day
WHERE
    yr_cd >= '2001'
    AND   yr_cd <= '2022'
GROUP BY
    yr_cd
UNION ALL
SELECT
    yr_cd,
    qtr_cd,
    NULL AS mnth_cd,
    NULL AS wk_cd,
    CAST (COUNT(1) AS INT) AS sys_calendar_days
FROM
    t_dim_day
WHERE
    yr_cd >= '2001'
    AND   yr_cd <= '2022'
GROUP BY
    yr_cd,
    qtr_cd
UNION ALL
SELECT
    yr_cd,
    qtr_cd,
    mnth_cd,
    NULL AS wk_cd,
    CAST (COUNT(1) AS INT) AS sys_calendar_days
FROM
    t_dim_day
WHERE
    yr_cd >= '2001'
    AND   yr_cd <= '2022'
GROUP BY
    yr_cd,
    qtr_cd,
    mnth_cd
UNION ALL
SELECT
    yr_cd,
    NULL AS qtr_cd,
    NULL AS mnth_cd,
    wk_cd,
    CAST (COUNT(1) AS INT) AS sys_calendar_days
FROM
    t_dim_day
WHERE
    yr_cd >= '2001'
    AND   yr_cd <= '2022'
GROUP BY
    yr_cd,
    wk_cd
ORDER BY
    yr_cd DESC,
    qtr_cd DESC,
    mnth_cd DESC,
    wk_cd DESC