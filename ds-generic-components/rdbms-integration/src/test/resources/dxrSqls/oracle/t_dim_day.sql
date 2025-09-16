SELECT
    t.*
FROM
    t_dim_day t
WHERE
    yr_cd >= '2001'
    AND   yr_cd <= '2022'