SELECT 
	name 
FROM system.columns
WHERE database = %(db)s AND table = %(logTable)s; 