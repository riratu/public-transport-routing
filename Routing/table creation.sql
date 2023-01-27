CREATE TABLE stops
(
    stop_id             int PRIMARY KEY NOT NULL,
    stop_code           TEXT,
    stop_name           TEXT,
    stop_desc           TEXT,
    stop_lat            numeric,
    stop_lon            decimal,
    zone_id             integer,
    stop_url            TEXT,
    location_type       INT,
    parent_station      INT,
    stop_timezone       TEXT,
    wheelchair_boarding INT,
    level_id            INT,
    platform_code       INT
);

CREATE INDEX ON stops (stop_id);
CREATE INDEX ON stops (stop_name);

-- IMPORT
COPY stops (stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station)
    FROM '/Users/gianni/Documents/Projekte/privat/mountain/Routing/gtfs_fp2023_2022-12-07_08-31/stops.txt'
    DELIMITER ',' CSV HEADER;

CREATE TABLE stop_times
(
    id             serial,
    trip_id        TEXT NOT NULL,
    arrival_time   interval,
    departure_time interval,
    stop_id        TEXT,
    stop_sequence  TEXT,
    pickup_type    TEXT,
    drop_off_type  TEXT
);

CREATE INDEX ON stop_times (trip_id);
CREATE INDEX ON stop_times (stop_id);
CREATE INDEX ON stop_times (departure_time);

-- IMPORT
COPY stop_times (trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type)
    FROM '/Users/gianni/Documents/Projekte/privat/mountain/Routing/gtfs_fp2023_2022-12-07_08-31/stop_times.txt'
    DELIMITER ',' CSV HEADER;

ALTER TABLE stop_times ALTER COLUMN arrival_time TYPE INTERVAL USING arrival_time::interval;
ALTER TABLE stop_times ALTER COLUMN departure_time TYPE INTERVAL USING departure_time::interval;



CREATE TABLE transfers
(
    id                serial PRIMARY KEY NOT NULL,
    from_stop_id      TEXT               NOT NULL,
    to_stop_id        TEXT               NOT NULL,
    transfer_type     TEXT,
    min_transfer_time int
);

-- IMPORT
COPY transfers (from_stop_id, to_stop_id, transfer_type, min_transfer_time)
    FROM '/Users/gianni/Documents/Projekte/privat/mountain/Routing/gtfs_fp2023_2022-12-07_08-31/transfers.txt'
    DELIMITER ',' CSV HEADER;

CREATE INDEX ON transfers (from_stop_id);
CREATE INDEX ON transfers (to_stop_id);

CREATE TABLE trips
(
    id              serial PRIMARY KEY NOT NULL,
    route_id        TEXT,
    service_id      TEXT               NOT NULL,
    trip_id         TEXT               NOT NULL,
    trip_headsign   TEXT,
    trip_short_name TEXT,
    direction_id    TEXT,
    block_id TEXT
);

CREATE INDEX ON trips (route_id);
CREATE INDEX ON trips (trip_id);

CREATE INDEX ON trips (trip_id, service_id, block_id);
CREATE INDEX ON trips (block_id, service_id, trip_id );

drop index trips_service_id_block_id_idx;
-- IMPORT
COPY trips (route_id, service_id, trip_id, trip_headsign, trip_short_name, direction_id, block_id)
    FROM '/Users/gianni/Documents/Projekte/privat/mountain/Routing/gtfs_fp2023_2022-12-07_08-31/trips.txt'
    DELIMITER ',' CSV HEADER;

CREATE TABLE routes
(
    id               serial PRIMARY KEY NOT NULL,
    route_id         TEXT               NOT NULL,
    agency_id        TEXT,
    route_short_name TEXT               NOT NULL,
    route_long_name  TEXT,
    route_desc       TEXT,
    route_type       TEXT
);

CREATE INDEX ON routes (route_id);

-- IMPORT
COPY routes (route_id, agency_id, route_short_name, route_long_name, route_desc, route_type)
    FROM '/Users/gianni/Documents/Projekte/privat/mountain/Routing/gtfs_fp2023_2022-12-07_08-31/routes.txt'
    DELIMITER ',' CSV HEADER;

CREATE TABLE calendar
(
    id         serial PRIMARY KEY NOT NULL,
    service_id TEXT,
    monday     TEXT,
    tuesday    TEXT               ,
    wednesday  TEXT,
    thursday   TEXT,
    friday     TEXT,
    saturday   TEXT,
    sunday     TEXT,
    start_date TEXT,
    end_date   TEXT
);
CREATE INDEX ON calendar (service_id, start_date, end_date);
CREATE INDEX ON calendar (start_date, end_date);

COPY calendar (
             service_id,
             monday    ,
             tuesday                  ,
             wednesday ,
             thursday  ,
             friday    ,
             saturday  ,
             sunday    ,
             start_date,
             end_date
    )
    FROM '/Users/gianni/Documents/Projekte/privat/mountain/Routing/gtfs_fp2023_2022-12-07_08-31/calendar.txt'
    DELIMITER ',' CSV HEADER;

CREATE TABLE calendar_dates
(
    id         serial PRIMARY KEY NOT NULL,
    date date,
    service_id     TEXT,
    exception_type    int
);

CREATE INDEX ON calendar_dates (service_id);
CREATE INDEX ON calendar_dates (date);

COPY calendar_dates (
               service_id    ,
               "date",
               "exception_type"
    )
    FROM '/Users/gianni/Documents/Projekte/privat/mountain/Routing/gtfs_fp2023_2022-12-07_08-31/calendar_dates.txt'
    DELIMITER ',' CSV HEADER;


---------------------------------

DROP MATERIALIZED VIEW graph;

CREATE MATERIALIZED VIEW graph
AS
select Split_part (stops.stop_id, ':', 1) as stop_id_short, stops.stop_id,  stops.stop_name, trips.trip_id, routes.route_id, trips.direction_id, trips.block_id, stop_times.departure_time::time, stop_times.arrival_time::time, route_short_name, trips.trip_headsign, trips.service_id
from trips
         join routes on trips.route_id = routes.route_id
         join stop_times on trips.trip_id = stop_times.trip_id
         join stops on stop_times.stop_id = stops.stop_id;

create index on graph (trip_id, departure_time);
create index on graph (stop_id, departure_time);
create index on graph (arrival_time);
create index on graph (departure_time, stop_id);
create index on graph (stop_id, departure_time, block_id);

create index on graph (service_id, departure_time, stop_id);

create index on graph (block_id);

create index on graph (stop_id, departure_time, stop_id_short, stop_id, stop_name, trip_id, route_id, direction_id, arrival_time, route_short_name, trip_headsign);
