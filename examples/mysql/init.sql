-- Run once, as root, when the container first starts.
--
-- MYSQL_DATABASE grants the example user rights on `analytics` and nothing
-- else, so the `reporting` database a materialization writes to has to be
-- created and granted here. Without this the example fails at seed time with
-- "Access denied for user 'gantry'@'%' to database 'reporting'".
CREATE DATABASE IF NOT EXISTS reporting;
GRANT ALL PRIVILEGES ON reporting.* TO 'gantry'@'%';
FLUSH PRIVILEGES;
