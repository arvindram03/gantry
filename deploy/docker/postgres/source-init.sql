-- Source-side setup for CDC. The Movement's Prepare stage creates the
-- publication and replication slot at plan time; this only grants the
-- privileges that require superuser at bootstrap.
ALTER SYSTEM SET wal_level = 'logical';
ALTER ROLE gantry WITH REPLICATION;
