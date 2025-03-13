SELECT 
	name 
FROM system.tables
WHERE database = %(db)s AND name = %(logTable)s; 