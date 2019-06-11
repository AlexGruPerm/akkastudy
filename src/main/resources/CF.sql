drop table mts_src.ticks;
drop table mts_src.ticks_count_days;
drop table mts_src.ticks_count_total;

CREATE TABLE mts_src.ticks_count_days(
	ticker_id int,
	ddate date,
	ticks_count counter,
	PRIMARY KEY (ticker_id, ddate)
) WITH CLUSTERING ORDER BY ( ddate DESC );

CREATE TABLE mts_src.ticks_count_total(
	ticker_id int,
	ticks_count counter,
	PRIMARY KEY (ticker_id));

CREATE TABLE mts_src.ticks(
	ticker_id int,
	ddate date,
	ts bigint,
	db_tsunx bigint,
	ask double,
	bid double,
	PRIMARY KEY ((ticker_id, ddate ), ts, db_tsunx)
) WITH CLUSTERING ORDER BY (ts DESC, db_tsunx DESC)