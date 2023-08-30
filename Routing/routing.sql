


-- find trips from a given location and time
select trips.trip_id, stops.stop_name, stop_times.departure_time::time, route_short_name, trips.trip_headsign
from trips
join routes on trips.route_id = routes.route_id
     join stop_times on trips.trip_id = stop_times.trip_id
AND stop_times.departure_time > interval '11:11'
AND stop_times.departure_time < (interval '11:11' + interval'1 hour')
     join stops on stop_times.stop_id = stops.stop_id and stops.stop_name = 'Lausen, Ziegelmatt';

-- find stops all transfers from a stop
select stops.stop_name, transfers.* from stops
                                             join transfers on stops.stop_id = transfers.from_stop_id
where stops.stop_name = 'Olten';

-- find all stops from a trip
select *, Split_part (stops.stop_id, ':', 1) from trips
                                                      join stop_times on trips.trip_id = stop_times.trip_id
                                                      join stops on stops.stop_id = stop_times.stop_id
where trips.trip_id = '144.TA.91-3-B-j23-1.91.H';


--------NON recursive Part----------------------------
--find the stops of a trip
--     select Split_part(stops.stop_id, ':', 1), stop_times.arrival_time::time, trip_id
--     from stop_times
--              join stops
--                   on stop_times.stop_id = stops.stop_id
--     where stop_times.trip_id IN (
--         -- find trips from a given location and time
--         select trips.trip_id
--         from trips
--                  join routes on trips.route_id = routes.route_id
--                  join stop_times on trips.trip_id = stop_times.trip_id
--                  join stops on stop_times.stop_id = stops.stop_id
--                  join all_the_stops on stop_times.departure_time > all_the_stops.arrival_time
--                                            and (stop_times.departure_time - interval '1 hour') < all_the_stops.arrival_time
--             and Split_part(stops.stop_id, ':', 1) = all_the_stops.stop_id_bhf))



--select stop id
-- join trips on stop_times.trip_id
-- select all the trips
-- from a stop
WITH RECURSIVE all_the_stops AS
    (select stop_id as the_stop, t.trip_id as the_trip, t.trip_headsign
     from stop_times
     join trips t on stop_times.trip_id = t.trip_id
     where Split_part(stop_times.stop_id, ':', 1) = '8578952'
     UNION
select stop_id, t.trip_id, t.trip_headsign from stop_times
join trips t on stop_times.trip_id = t.trip_id
join all_the_stops on Split_part(stop_times.stop_id, ':', 1) = all_the_stops.the_trip)
select * from all_the_stops;

----------------

select * from stop_times
join trips on stop_times.trip_id = trips.trip_id
where stop_times.stop_id = '8578952';


select the stops and the
select Split_part(stops.stop_id, ':', 1), stop_times.arrival_time::time, trip_id
from stop_times
         join stops
              on stop_times.stop_id = stops.stop_id
join trips
             join routes on trips.route_id = routes.route_id
             join stop_times on trips.trip_id = stop_times.trip_id
             join stops on stop_times.stop_id = stops.stop_id;



             join all_the_stops on stop_times.departure_time > all_the_stops.arrival_time
        and (stop_times.departure_time - interval '1 hour') < all_the_stops.arrival_time
        and Split_part(stops.stop_id, ':', 1) = all_the_stops.stop_id_bhf))

----------------------------
-----create function-------|
----------------------------

CREATE OR REPLACE FUNCTION getStopsOfTrips(depth int, currentStop text, funct_departure_time time)
    RETURNS table(stop_id text, arrival_time time, trip_id text) AS $nextStops$
declare
    depth integer;
    currentStop text;
    funct_departure_time time;
BEGIN
    if depth > 5 THEN
    RETURN;
    END IF;
    depth = depth + 1;
    return QUERY select Split_part(stops.stop_id, ':', 1), stop_times.arrival_time::time, stop_times.trip_id
    from stop_times
             join stops
                  on stop_times.stop_id = stops.stop_id
    where stop_times.trip_id IN (
        -- find trips from a given location and time
        select trips.trip_id
        from trips
                 join routes on trips.route_id = routes.route_id
                 join stop_times on trips.trip_id = stop_times.trip_id
--                                 and stop_times.departure_time > funct_departure_time
--                                 and (stop_times.departure_time - interval '1 hour') < funct_departure_time
                 join stops on stop_times.stop_id = stops.stop_id
                                and Split_part(stops.stop_id, ':', 1) = currentStop);
END;
$nextStops$ LANGUAGE plpgsql;

select getStopsOfTrips(1, '8578952', '13:00:00'::time);

-----testtable function
CREATE OR REPLACE FUNCTION testTable(depth int, currentStop text, funct_departure_time time)
    RETURNS table(stop_id text, arrival_time time, trip_id text) AS $nextStops$
declare
    depth integer;
    currentStop text;
    funct_departure_time time;
BEGIN
   select 'eins', '13:00:00'::time, 'drei' into nextStops FROM EMP;
END;
$nextStops$ LANGUAGE plpgsql;

explain (ANALYZE, BUFFERS, Verbose)  select trips.trip_id,
       trips.block_id,
       trips.service_id,
       transfers.min_transfer_time,
       stop_times.departure_time,
       trips.trip_headsign,
       extract('ISODOW' from :departureDate::date) as weekday
from trips
        left join stop_times on stop_times.trip_id = trips.trip_id
    and (stop_times.departure_time
        BETWEEN :departureTime
        and (:departureTime + interval '+ 1 hour'))
    --when is is not the same vehicle, factor the transfer time in
         left join transfers on transfers.from_stop_id = :startStopId and transfers.to_stop_id = stop_id
where
((stop_times.stop_id IN
    (select to_stop_id from transfers where transfers.from_stop_id = :startStopId))
    or
  stop_times.stop_id = :startStopId);

--- updated     query
EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) select graph.trip_id,
       graph.block_id,
       graph.departure_time,
       graph.trip_headsign
from graph
         left join calendar_dates
                   on graph.service_id = calendar_dates.service_id
                       and calendar_dates.date = :departureDate::date
join calendar
              on graph.service_id = calendar.service_id
                     --check if trip is available at that weekday
                  and (CASE extract('ISODOW' from :departureDate::date)
                            WHEN 1 THEN calendar.monday = '1'
                            WHEN 2 THEN calendar.tuesday = '1'
                            WHEN 3 THEN calendar.wednesday = '1'
                            WHEN 4 THEN calendar.thursday = '1'
                            WHEN 5 THEN calendar.friday = '1'
                            WHEN 6 THEN calendar.saturday = '1'
                            WHEN 7 THEN calendar.sunday = '1'
                            END)
                            --check if trip is in date range
                  and :departureDate::date >= calendar.start_date::date
                  and :departureDate::date <= calendar.end_date::date
                        --check if trip is not excepted at this date
where
    -- (
    stop_id IN
        (select to_stop_id from transfers where transfers.from_stop_id = :startStopId)
    -- or
    --     stop_id = :startStopId
    -- )
and
    graph.departure_time BETWEEN :departureTime
    and (:departureTime + interval '+ 1 hour')
ORDER BY departure_time;

-- simple to test
EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
select graph.trip_id,
       graph.block_id,
       graph.departure_time,
       graph.trip_headsign
from graph
         join calendar
              on graph.service_id = calendar.service_id
                  --check if trip is available at that weekday
                  and (CASE extract('ISODOW' from :departureDate::date)
                           WHEN 1 THEN calendar.monday = '1'
                           WHEN 2 THEN calendar.tuesday = '1'
                           WHEN 3 THEN calendar.wednesday = '1'
                           WHEN 4 THEN calendar.thursday = '1'
                           WHEN 5 THEN calendar.friday = '1'
                           WHEN 6 THEN calendar.saturday = '1'
                           WHEN 7 THEN calendar.sunday = '1'
                      END)
                  --check if trip is in date range
                  and :departureDate::date >= calendar.start_date::date
                  and :departureDate::date <= calendar.end_date::date
     --check if trip is not excepted at this date
where
        stop_id IN
        (select to_stop_id from transfers where transfers.from_stop_id = :startStopId)-- (
-- or
--     stop_id = :startStopId
-- )
  and graph.departure_time BETWEEN :departureTime
    and (:departureTime + interval '+ 1 hour');


EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON)
select stop_times.trip_id,
       stop_times.departure_time
INTO TEMP EINS
    from stop_times
     --check if trip is not excepted at this date
where
        stop_id IN
        (select to_stop_id from transfers where transfers.from_stop_id = :startStopId)-- (
-- or
--     stop_id = :startStopId
-- )
  and departure_time BETWEEN :departureTime
    and (:departureTime + interval '+ 1 hour');
select * from EINS join trips using (trip_id)
                            join calendar
                                 on trips.service_id = calendar.service_id
                                     --check if trip is available at that weekday
                                     and (CASE extract('ISODOW' from :departureDate::date)
                                              WHEN 1 THEN calendar.monday = '1'
                                              WHEN 2 THEN calendar.tuesday = '1'
                                              WHEN 3 THEN calendar.wednesday = '1'
                                              WHEN 4 THEN calendar.thursday = '1'
                                              WHEN 5 THEN calendar.friday = '1'
                                              WHEN 6 THEN calendar.saturday = '1'
                                              WHEN 7 THEN calendar.sunday = '1'
                                         END)
                                     --check if trip is in date range
                                     and :departureDate::date >= calendar.start_date::date
                                     and :departureDate::date <= calendar.end_date::date;


-- einszwei drei
explain analyse select trips.trip_id,
       trips.block_id,
       stop_times.departure_time,
       trips.trip_headsign
from trips
         join stop_times on trips.trip_id = stop_times.trip_id
    and (stop_times.departure_time
        BETWEEN :departureTime
        and (:departureTime + interval '+ 1 hour'))
         join calendar_dates on trips.service_id = calendar_dates.service_id
                                    and calendar_dates.date = :departureDate::date
                                    and (calendar_dates.exception_type <> '2' or calendar_dates.exception_type is null
                                    ) or calendar_dates.exception_type = '1'
         join calendar
              on trips.service_id = calendar.service_id
                  --check if trip is available at that weekday
                  and (CASE extract('ISODOW' from :departureDate::date)
                           WHEN 1 THEN calendar.monday = '1'
                           WHEN 2 THEN calendar.tuesday = '1'
                           WHEN 3 THEN calendar.wednesday = '1'
                           WHEN 4 THEN calendar.thursday = '1'
                           WHEN 5 THEN calendar.friday = '1'
                           WHEN 6 THEN calendar.saturday = '1'
                           WHEN 7 THEN calendar.sunday = '1'
                      END)
                  --check if trip is in date range
                  and :departureDate::date > calendar.start_date::date
                  and :departureDate::date < calendar.end_date::date
     --check if trip is not excepted at this date
where
        stop_id IN
        (select to_stop_id from transfers where transfers.from_stop_id = :startStopId)
ORDER BY departure_time;

