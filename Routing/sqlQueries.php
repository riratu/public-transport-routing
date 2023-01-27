<?php
function getStopsFromTripQuery()
{
    return '
select trip_id, block_id, stop_id, stop_id_short, stop_name, arrival_time, trip_headsign from graph
where trip_id = :tripId and departure_time > :startTime; --hier transfer table einbauen';
}

function getNextTripsQuery()
{
    return '
select trips.trip_id,
       trips.block_id,
       stop_times.departure_time,
       trips.trip_headsign
from trips
    
    join stop_times on trips.trip_id = stop_times.trip_id
    and (stop_times.departure_time
                    BETWEEN :departureTime
    and (:departureTime + interval \'+ 1 hour\'))
     
    join calendar_dates on trips.service_id = calendar_dates.service_id
                and calendar_dates.date = :departureDate::date
                and (calendar_dates.exception_type <> \'2\' or calendar_dates.exception_type is null
                     ) or calendar_dates.exception_type = \'1\'
     join calendar
              on trips.service_id = calendar.service_id
                     --check if trip is available at that weekday
                  and (CASE extract(\'ISODOW\' from :departureDate::date)
                            WHEN 1 THEN calendar.monday = \'1\'
                            WHEN 2 THEN calendar.tuesday = \'1\'
                            WHEN 3 THEN calendar.wednesday = \'1\'
                            WHEN 4 THEN calendar.thursday = \'1\'
                            WHEN 5 THEN calendar.friday = \'1\'
                            WHEN 6 THEN calendar.saturday = \'1\'
                            WHEN 7 THEN calendar.sunday = \'1\'
                            END)
                            --check if trip is in date range
                  and :departureDate::date > calendar.start_date::date
                  and :departureDate::date < calendar.end_date::date
                        --check if trip is not excepted at this date
where
    ((stop_id IN
    (select to_stop_id from transfers where transfers.from_stop_id = :startStopId)
    )
    or 
    stop_id = :startStopId
    )
ORDER BY departure_time;';
}