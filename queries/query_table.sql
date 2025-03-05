SELECT 
	name 
from system.tables
where database = %(db)s and name = %(table)s;

