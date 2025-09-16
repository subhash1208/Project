-- parquet file created using this sql

create table cdldsoletim.employee_monthly_shapetest_dataset
stored as parquet as
select * from (

	select 
	    *, '1' as ignored_column 
	from 
	    livedsmaindc1.employee_monthly
	where 
	    yyyymm = '201812'
	limit 1000
	
	union all 
	
	select 
	    *, '1' as ignored_column
	from 
	    livedsmaindc1.employee_monthly
	where 
	    yyyymm = '201901'
	limit 1000
	
	union all 
	
	select 
	    *, '1' as ignored_column
	from 
	    livedsmaindc1.employee_monthly
	where 
	    yyyymm = '201902'
	limit 1000
	
	union all 
	
	select 
	    *, '1' as ignored_column
	from 
	    livedsmaindc1.employee_monthly
	where 
	    yyyymm = '201903'
	limit 1000
) a
distribute by ignored_column;