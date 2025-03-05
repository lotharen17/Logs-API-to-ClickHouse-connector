SELECT 
	name
FROM system.tables
where database = %(db)s and name = %(table)s; 


