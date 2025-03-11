CREATE TABLE IF NOT EXISTS %(db)s.%(logTable)s
(	
	datetime 			DateTime NOT NULL, 
	response 			INT NOT NULL,
	endpoint			String NOT NULL, 
	description			String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(datetime)
ORDER BY (datetime, response, endpoint);
