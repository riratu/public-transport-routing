<?php

//xdebug_info();

$didokArray = parseCsvToArray('dienststellen_full_nur_brauchbare_spalten.csv', ';');

$path = "./crawler/results_olten/";
$resultArray = [];

//Resultfile
$fp = fopen('oltentomountains.kml', 'a');

$begin = <<<EOD
<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://www.opengis.net/kml/2.2 https://developers.google.com/kml/schema/kml22gx.xsd">
<Document>
<name>Von Olten in die Berge</name>
EOD;
fwrite($fp, $begin);

if ($handle = opendir($path)) {
    echo "Verzeichnis-Handle:$handle\n";
    echo "Einträge:\n";
    $i = 0;
    while (false !== ($jsonFile = readdir($handle))) {
        //if ($i > 10) break;
        $i++;
        if ($jsonFile != "." && $jsonFile != "..") {

            echo "$jsonFile\n";
            echo $path . "$jsonFile\n";
            $theFile = file_get_contents($path . $jsonFile);
            $content = json_decode($theFile);

            $i++;
            $connections = $content->connections;
            $duration = null;

            //find the shortest connection
            foreach ($connections as $connection) {
                //echo 'Current Duration: ' . $connection->duration . PHP_EOL;
                if (!isset($duration)) {
                    $duration = $connection->duration;
                } else {
                    if ($connection->duration < $duration) {
                        $duration = $connection->duration;
                    }
                }
            }
            if (isset($duration)) {
                $durationStripped = str_replace('00d', '', $duration);
            } else {
                continue;
                $durationStripped = 'Nix gefunden';
            }

            $bahnhofId = str_replace('.json', '', $jsonFile);

            echo $didokArray[$bahnhofId]['BEZEICHNUNG_OFFIZIELL'] . ' -> Fastest Connection: ' . $duration . PHP_EOL;

            $bezeichnung = $didokArray[$bahnhofId]['BEZEICHNUNG_OFFIZIELL'];
            $eCoordinate = $didokArray[$bahnhofId]['E_WGS84'];
            $nCoordinate = $didokArray[$bahnhofId]['N_WGS84'];

            if (!$eCoordinate) continue;

            $color = $color = sprintf("#%02x%02x%02x", 13, 0, 255); // #0d00ff

            $placemark = <<<EOT
<Placemark id="annotation'$bahnhofId'">
    <ExtendedData>
        <Data name="type">
            <value>annotation</value>
        </Data>
    </ExtendedData>
    <name>$durationStripped</name>
    <description>$bezeichnung</description>
    <Style>
        <IconStyle>
            <scale>0</scale>
        </IconStyle>
        <LabelStyle>
            <color>$color</color>
        </LabelStyle>
    </Style>
    <Point>
        <tessellate>1</tessellate>
        <altitudeMode>clampToGround</altitudeMode>
        <coordinates>$eCoordinate,$nCoordinate</coordinates>
    </Point>
  </Placemark>
EOT;
            fwrite($fp, $placemark);
        }
    }

    $end = <<<EOD
</Document>
</kml>
EOD;
    fwrite($fp, $end);

    fclose($fp);
    closedir($handle);
}

function parseCsvToArray($file, $separ = ';')
{
    $arrays = array_map(function ($foo) use ($separ) {
        return array_map("trim", str_getcsv($foo, $separ));
    }, file($file, FILE_SKIP_EMPTY_LINES));

    $header = $arrays[0];
    //unset($arrays[0]);

    $array_with_keys = [];
    $array_with_keys[] = $header;
    foreach ($arrays as $array) {
        $_array = [];
        foreach ($array as $key => $value) {
            $_array[$header[$key]] = $value;
        }
        $array_with_keys[$array[0]] = $_array;
    }

    return $array_with_keys;
}