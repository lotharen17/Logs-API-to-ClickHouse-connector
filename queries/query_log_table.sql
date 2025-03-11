SELECT 
	name 
FROM system.tables
WHERE database = %(db)s and name = %(logTable)s;
