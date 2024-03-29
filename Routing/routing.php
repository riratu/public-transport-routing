<?php

include('sqlQueries.php');
// NEXT:
// Todo: Graph mit allen Routen und Minimalzeit machen?
// Todo: Anzahl umsteigen in Graph speichern. Mit Route-Dingens.
// Todo: Problem mit Tageswechsel lösen.
// Todo: Block ID einbeziehen
//nächste Trips nach Cicle Id suchen.

$db = new PDO('pgsql:dbname=tt23;host=localhost');

$getStopsFromTripQuery = $db->prepare(getStopsFromTripQuery());

$getNextTripsQuery = $db->prepare(getNextTripsQuery());

// -------- Initial Values  -----------------------

//$startTime = new DateTime('2023-01-08 13:00:00'); //Default Starttime


//$startId = '8500218'; //Olten
$endStop = '8509253'; //St Moritz
//$startId = '8509002'; //Landquart
$startId = '8509068:0:3'; //Klosters Platz
$startTime = new DateTime('2023-01-08 15:55:00'); //Block Id Test

//Nearby Stations Test
//St. Moritz 8509253
//St. Moritz, Bahnhof 8574515:0:B
//Betten Talstation '8501675'
$startDate = $startTime->format('Y-m-d');
$visitedStops = [];
$traveledTrips = [];
$i = 0;
$calculationStart = time();

// -------- Get the first set of stops  -----------

$stopsToVisit[] = $startId;
$startIdShort = explode(":", $startId)[0];
$visitedStops[$startIdShort]['arrival_time'] = $startTime->format('H:i');
$visitedStops[$startIdShort]['stop_name'] = 'Startbahnhof';
$visitedStops[$startIdShort]['block_id'] = '1223';
$currentStopShort = null;

// -------- Travel the graph  ---------------------

while (!empty($stopsToVisit) && ($i < 1000000)){
    $currentStop = array_shift($stopsToVisit);
    $currentStopShort = explode(":", $currentStop)[0];
    //$departureTime = date('H:i',(strtotime($vertices[$currentStop]['arrival_time']) + strtotime('+10 minutes'));

    if ($currentStopShort === $endStop){
        echo 'reached End Stop';
        break;
    }

    $departureTime = $visitedStops[$currentStopShort]['arrival_time'];
    echo ">>>---- Get Trips From " . $visitedStops[$currentStopShort]['stop_name'] . " at $departureTime" .PHP_EOL;
    $nextTrips = getTripsFromStop($currentStop, $getNextTripsQuery, $traveledTrips, $startDate, $visitedStops);

     foreach ($nextTrips as $nextTrip){
        getStopsFromTrips($nextTrip['trip_id'] , $nextTrip['departure_time'], $getStopsFromTripQuery, $visitedStops, $stopsToVisit, $currentStop);
    }
    $i++;
    echo '-----  Stops to visit:' . count($stopsToVisit) . ' visited:' . count($visitedStops) . '-----------' . PHP_EOL;
}

// -------- Return Path to Destination  ------------

$bestConnection = '';
while (!empty($endStop) && $endStop != $startId){
    $prevStop = $visitedStops[$endStop]['last_stop'];
    $bestConnection = 'Von ' . $visitedStops[$prevStop]['stop_name'] . ' (' . $prevStop . ') Abfahrt später als ' . $visitedStops[$prevStop]['arrival_time'] . ' nach ' . $visitedStops[$endStop]['stop_name'] . ' (' . $endStop  .') Ankunft ' . $visitedStops[$endStop]['arrival_time'] . PHP_EOL . $bestConnection;
    $endStop = $prevStop;
}
echo $bestConnection;

// -------- Write the Times for all the Stops in a File. ---------

$fp = fopen('routingresult2.csv', 'a');
$resultData =  'to_stop,' . 'arrival_time,'. 'stop_name' . 'trip_id,' . 'last_stop,' . 'last_stop_id' . PHP_EOL;
fwrite($fp, $resultData);
foreach ($visitedStops as $stop){
    if (isset($stop['last_stop']) && isset($visitedStops[$stop['last_stop']])){
    $resultData = '"' . $stop['stop_id'] . '","' . $stop['arrival_time'] . '","' . $stop['stop_name'] . '","' . $stop['trip_id'] . '","' . $visitedStops[$stop['last_stop']]['stop_name'] . '","' . $stop['last_stop'] . '"'. PHP_EOL;
    fwrite($fp, $resultData);
    }
}

fclose($fp);

echo "Routing finished. Calculation took " . (time() - $calculationStart) . " Seconds";

/// --------- FUNCTIONS ---------

function getTripsFromStop($startId, $getNextTripsQuery, &$traveledTrips, $startDate, &$visitedStops){
    $nextTrips = [];
    $currentStopShort = explode(":", $startId)[0];

//    if ($currentStopShort === '8509068'){
//        echo 'Klosters Platz';
//    }
    $departureTime = $visitedStops[$currentStopShort]['arrival_time'];
    $blockId = $visitedStops[$currentStopShort]['block_id'];
    $getNextTripsQuery->execute(['departureTime' => $departureTime, 'departureDate' => $startDate, 'startStopId' => $startId]);
    while ($row = $getNextTripsQuery->fetch(PDO::FETCH_ASSOC)) {
        //if ($row['trip_id'] === null) continue;

        //Check if we traveled the trip before
        if (!isset($traveledTrips[$row['trip_id']])
            || $traveledTrips[$row['trip_id']]['departure_time'] < $row['departure_time']){

            $traveledTrips[$row['trip_id']]['departure_time'] = $row['departure_time'];
            $nextTrips[] = $row;
        }
    }
    return $nextTrips;
}

function getStopsFromTrips($trip_id, $startTime, $getStopsFromTripQuery, &$visitedStops, &$stopsToVisit, $currentStop)
{
    $getStopsFromTripQuery->execute(['tripId' => $trip_id, 'startTime' => $startTime]);

    while ($row = $getStopsFromTripQuery->fetch(PDO::FETCH_ASSOC)) {
        $currentStopShort = explode(":", $currentStop)[0];
        //echo('Trip ID ' . $row['trip_id'] . ' to ' . $row['trip_headsign'] . ' hält in ' . $row['stop_name'] . ' um ' . $row['arrival_time'] . PHP_EOL);

        //Stop not visited. Add it to the array to travel.
        if (empty($visitedStops[$row['stop_id_short']])) {
            echo $row['stop_name'] . ' not found in array. Adding it. Arrival ' . $row['arrival_time'] . PHP_EOL;
            $visitedStops[$row['stop_id_short']] = $row;
            $visitedStops[$row['stop_id_short']]['last_stop'] = $currentStopShort;
            $stopsToVisit[] = $row['stop_id'];
        } else {
            //Found a faster way to the stop. Adding it.
            if ($row['arrival_time'] < $visitedStops[$row['stop_id_short']]['arrival_time']) {

                //Debugging info
                echo $row['arrival_time'] . ' is smaller than ' . $visitedStops[$row['stop_id_short']]['arrival_time'] . ' adding stop ' . $row['stop_id_short'] . PHP_EOL;

                //Add Data to the visited Stops
                $visitedStops[$row['stop_id_short']] = $row;
                $visitedStops[$row['stop_id_short']]['last_stop'] = $currentStopShort;

                //Adding it to the Stops to visit if not existent
                if (!isset($stopsToTravel[$row['stop_id_short']])) {
                    $stopsToVisit[] = $row['stop_id'];
                }
            }
        }
    }
    return $visitedStops;
}
