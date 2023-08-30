--to perform on materialized view
-------
--get the next stops from a trip
select t.stop_id_short, t.arrival_time, s.trip_headsign from graph t
JOIN graph s ON  t.stop_id_short = s.stop_id_short  LIMIT 100;

--get the next trips from a stop and time
select trip_id from graph t where stop_id_short = '8578952'
                              and departure_time BETWEEN '13:00' and '14:00';


--get the next stops from a trip
explain (FORMAT JSON)
select t.stop_name, t.trip_headsign, s.stop_id_short, s.stop_name from graph s
join graph t on s.trip_id = t.trip_id and t.departure_time < s.arrival_time
where t.stop_id_short = '8500218' --hier transfer table einbauen
  and t.departure_time BETWEEN '13:00' and '14:00'; --and date weekday

select stop_name, stop_id_short from graph where stop_name = 'Olten';


----Recursive Part

Explain
WITH RECURSIVE all_the_stops AS
                   (select stop_id_short as from_stop_id, stop_name as from_stop_name, trip_id, trip_headsign, stop_id_short as to_stop_id_short, stop_name as to_stop_name, arrival_time
                    from graph
                    where stop_name = 'Olten' and arrival_time BETWEEN '13:00' and '14:00'
                    UNION
                    select t.stop_id_short, t.stop_name, s.trip_id, s.trip_headsign, s.stop_id_short, s.stop_name, s.arrival_time
                    from graph s
                             join graph t on s.trip_id = t.trip_id and t.departure_time < s.arrival_time
                   -- where t.stop_id_short = '8500218' --hier transfer table einbauen
                    --  and t.departure_time BETWEEN '13:00' and '14:00'; --and date weekday

join all_the_stops on all_the_stops.to_stop_id_short = s.stop_id_short and t.departure_time BETWEEN (all_the_stops.arrival_time + interval '10 minutes') and (all_the_stops.arrival_time + interval '1 hour') )

select *
from all_the_stops ORDER BY arrival_time DESC LIMIT 200;


--select all trips from to...
select t.stop_name, t.departure_time, t.trip_id, t.trip_headsign, s.stop_id_short, s.stop_name, s.arrival_time
from graph s
         join graph t on s.trip_id = t.trip_id and t.departure_time < s.arrival_time
where t.stop_id_short = '8500218' --hier transfer table einbauen
and t.departure_time BETWEEN '13:00' and '14:00'; --and date weekday

--stop, arrival time, trip id
select s.stop_name, s.arrival_time, s.trip_id, t.stop_name
from graph s
         join graph t on s.trip_id = t.trip_id and t.departure_time < s.arrival_time
where t.stop_id_short = '8500218'
  and t.departure_time BETWEEN '13:00' and '14:00'; --and date weekday


select t.stop_name, t.departure_time, t.trip_id, t.trip_headsign, s.stop_id_short, s.stop_name, s.arrival_time
from graph s
         join graph t on s.trip_id = t.trip_id and t.departure_time < s.arrival_time
where t.stop_id_short = :fromStop --hier transfer table einbauen
  and t.departure_time BETWEEN :departure and (:departure + interval '1 hour';


CREATE OR REPLACE FUNCTION day_name(day_index integer)
    RETURNS text
    LANGUAGE sql
    STRICT IMMUTABLE PARALLEL SAFE AS
$$
SELECT CASE day_index
           WHEN 0 THEN 'monday'
           WHEN 1 THEN 'tuesday'
           WHEN 2 THEN 'wednesday'
           WHEN 3 THEN 'thursday'
           WHEN 4 THEN 'friday'
           WHEN 5 THEN 'saturday'
           WHEN 6 THEN 'sunday'
           END;
$$;
select calendar.end_date::date
from calendar;


-- \set departureDateTime '\'2023-11-01 13:00\'::timestamp'
-- \set departureDateTime '\'8500218\''

select trip_id,
       transfers.min_transfer_time,
       departure_time,
       trip_headsign,
       calendar_dates.date                             as calender_date,
       calendar_dates.exception_type,
       extract('ISODOW' from '2023-11-05 13:00'::date) as weekday,
       calendar.sunday,
       calendar.start_date::date,
       calendar.end_date::date
from graph
         join transfers on transfers.from_stop_id = '8500218' and transfers.to_stop_id = stop_id
         left join calendar_dates on graph.service_id = calendar_dates.service_id
    and calendar_dates.date = '2023-11-05 13:00'::date
         join calendar
              on graph.service_id = calendar.service_id
                  --check if trip is available at that weekday
                  and ((CASE extract('ISODOW' from '2023-11-05 13:00'::date)
                            WHEN 1 THEN calendar.monday = '1'
                            WHEN 2 THEN calendar.tuesday = '1'
                            WHEN 3 THEN calendar.wednesday = '1'
                            WHEN 4 THEN calendar.thursday = '1'
                            WHEN 5 THEN calendar.friday = '1'
                            WHEN 6 THEN calendar.saturday = '1'
                            WHEN 7 THEN calendar.sunday = '1'
                            END
                      --check if trip is in date range
                      and '2023-11-05 13:00'::date > calendar.start_date::date
                      and '2023-11-05 13:00'::date < calendar.end_date::date
                      --check if trip is not exceptet at this date
                      and (calendar_dates.exception_type <> '2' or calendar_dates.exception_type is null
                            )
                           --check if trip is an exception at this date
                           ) or calendar_dates.exception_type = '1')
where departure_time
    BETWEEN (CAST('2023-11-05 13:00'::timestamp AS TIME(0)) + make_interval(secs => transfers.min_transfer_time))
    and (CAST('2023-11-05 13:00'::timestamp AS TIME(0)) + interval '+ 1 hour')
  and stop_id IN
      (select to_stop_id from transfers where from_stop_id = '8500218')
ORDER BY departure_time;

select *
from trips
where trip_id = '112.TA.92-503-A-j23-1.7.R';
select *
from calendar_dates
where service_id = (select service_id from trips where trip_id = '112.TA.92-503-A-j23-1.7.R');


select *
from calendar
where service_id = (select service_id from trips where trip_id = '112.TA.92-503-A-j23-1.7.R');



---- Test The Block ID Zeugs
select *
from stop_times
         join stops on stop_times.stop_id = stops.stop_id
where trip_id = '103.TA.91-1P-Y-j23-1.41.R';

select *
from trips
ORDER BY block_id desc;

select *
from graph
where trip_id = '17.TA.91-25-Y-j23-1.13.R';

select *
from trips
where block_id = '1223';

select *
from graph
where (stop_id IN (select to_stop_id from transfers where transfers.from_stop_id = '8509068:0:3')
    or stop_id = '8509068:0:3')
    and
      (departure_time
          BETWEEN ('15:59')
          and ('15:59' + interval '+ 1 hour'))
ORDER BY departure_time;

select * from graph where block_id != '' and trip_id = ;

select * from transfers where transfers.from_stop_id = '8509068:0:3';

select * from graph
join transfers on transfers.from_stop_id = '8594551' and transfers.to_stop_id = stop_id
where (departure_time
          BETWEEN ('15:59' + make_interval(secs => transfers.min_transfer_time))
          and ('15:59' + interval '+ 1 hour'))
      or
(departure_time
    BETWEEN '15:59' and ('15:59' + interval '+ 1 hour')
    and graph.block_id = '1223')
and stop_id IN
      (select to_stop_id from transfers where transfers.from_stop_id = '8594551') ORDER BY departure_time;

---optimisation index test (next check why it is so slow after e join left)
explain analyse select graph.trip_id,
               graph.block_id,
               graph.service_id,
               transfers.min_transfer_time,
               departure_time,
               trip_headsign,
               calendar_dates.date as calender_date,
               calendar_dates.exception_type,
               extract('ISODOW' from '2023-01-08'::date) as weekday,
               calendar.sunday,
               calendar.start_date::date,
               calendar.end_date::date
        from graph
                 left join calendar_dates on graph.service_id = calendar_dates.service_id
            and calendar_dates.date = :departureDate::date
                 join calendar
                      on graph.service_id = calendar.service_id
                          --check if trip is available at that weekday
                          and ((CASE extract('ISODOW' from '2023-01-08'::date)
                                    WHEN 1 THEN calendar.monday = '1'
                                    WHEN 2 THEN calendar.tuesday = '1'
                                    WHEN 3 THEN calendar.wednesday = '1'
                                    WHEN 4 THEN calendar.thursday = '1'
                                    WHEN 5 THEN calendar.friday = '1'
                                    WHEN 6 THEN calendar.saturday = '1'
                                    WHEN 7 THEN calendar.sunday = '1'
                                    END
                              --check if trip is in date range
                              and :departureDate::date > calendar.start_date::date
                              and :departureDate::date < calendar.end_date::date
                              --check if trip is not exceptet at this date
                              and (calendar_dates.exception_type <> '2' or calendar_dates.exception_type is null
                                    )
                                   --check if trip is an exception at this date
                                   ) or calendar_dates.exception_type = '1')
                left join transfers on (transfers.from_stop_id = :startStopId and transfers.to_stop_id = stop_id)
        where stop_id = :startStopId
        and
            (departure_time
            BETWEEN (:departureTime + make_interval(secs => transfers.min_transfer_time))
            and (:departureTime + interval '+ 1 hour'))
                  ORDER BY departure_time;

--get all the possible ids with the transfers



---optimisation index test (next check why it is so slow after e join left)
select graph.trip_id,
       graph.block_id,
       graph.service_id,
       transfers.min_transfer_time,
       departure_time,
       trip_headsign,
                       (CASE WHEN graph.block_id = :block_id and graph.block_id != '' THEN
                                     departure_time > (:departureTime + make_interval(secs => transfers.min_transfer_time))
                           END)
from graph
         left join transfers on (transfers.from_stop_id = :startStopId
                                     and transfers.to_stop_id = stop_id)
                where stop_id = :startStopId
                  and (departure_time
                    BETWEEN (:departureTime)
                    and (:departureTime + interval '+ 1 hour'));


explain analyse select graph.trip_id,
       graph.block_id,
       graph.service_id,
       transfers.min_transfer_time,
       departure_time,
       trip_headsign,
       calendar_dates.date as calender_date,
       calendar_dates.exception_type,
       extract('ISODOW' from '2023-01-08'::date) as weekday,
       calendar.sunday,
       calendar.start_date::date,
       calendar.end_date::date
from graph
         left join transfers on transfers.from_stop_id = :startStopId and transfers.to_stop_id = stop_id
         left join calendar_dates on graph.service_id = calendar_dates.service_id
    and calendar_dates.date = '2023-01-08'::date
         join calendar
              on graph.service_id = calendar.service_id
                  --check if trip is available at that weekday
                  and ((CASE extract('ISODOW' from '2023-01-08'::date)
                            WHEN 1 THEN calendar.monday = '1'
                      WHEN 2 THEN calendar.tuesday = '1'
                      WHEN 3 THEN calendar.wednesday = '1'
                      WHEN 4 THEN calendar.thursday = '1'
                      WHEN 5 THEN calendar.friday = '1'
                      WHEN 6 THEN calendar.saturday = '1'
                      WHEN 7 THEN calendar.sunday = '1'
                      END
                      --check if trip is in date range
                      and '2023-01-08'::date > calendar.start_date::date
                      and '2023-01-08'::date < calendar.end_date::date
                      --check if trip is not exceptet at this date
                      and (calendar_dates.exception_type <> '2' or calendar_dates.exception_type is null
                      )
                           --check if trip is an exception at this date
                           ) or calendar_dates.exception_type = '1')
where
(departure_time
        BETWEEN :departureTime
        and (:departureTime + interval '+ 1 hour'))
    --when is is not the same vehicle, factor the transfer time in
  and ((stop_id IN
    (select to_stop_id from transfers where transfers.from_stop_id = :startStopId)
)
    or
    stop_id = :startStopId
)
  and (CASE
           WHEN :block_id = '' or :block_id != graph.block_id  then
    graph.departure_time > (:departureTime + make_interval(secs => transfers.min_transfer_time))
           else
               true
    END)
            ORDER BY departure_time;