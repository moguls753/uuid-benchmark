-- Grant privileges to benchmark user for reading InnoDB metrics and stats
GRANT PROCESS ON *.* TO 'benchmark'@'%';
GRANT SELECT ON mysql.innodb_index_stats TO 'benchmark'@'%';
GRANT SELECT ON mysql.innodb_table_stats TO 'benchmark'@'%';
FLUSH PRIVILEGES;
