select *
	, map('somekey', map('k1', 10, 'k2', 20) , 'another', map('k3',30)) as mymap
	, struct('value1' as field1, 'value2' as field2, 20 as field3, cast(null as int) as field4) as mystruct
	, array(map('k1','v1'), map('k2', 'v2')) as myarray
from cdldsraw1.emp_data 
where name is not null and ooid is not null