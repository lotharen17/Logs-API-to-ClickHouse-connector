SELECT
	name
FROM system.columns
WHERE database = %(db)s and table = %(table)s; 
