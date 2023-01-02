<?php

//xdebug_info();

$didokArray = parseCsvToArray('dienststellen_full_nur_brauchbare_spalten.csv', ';');

$path = "./results_new/";
$resultArray = [];

//resultfile
$fp = fopen('station_list_with_durations.csv', 'a');

$spaltenArray = [
    'BEZEICHNUNG_OFFIZIELL',
    'Dauer',
    'Z_LV03',
    'BPUIC',
    'BPVH_VERKEHRSMITTEL_TEXT_EN',
    'E_WGS84',
    'N_WGS84',
    'KANTONSNAME',
    'BHFID2'
];

$didokBahnhofWerte = '"' . implode('","', $spaltenArray) . '"';
$resultData = $didokBahnhofWerte . PHP_EOL;
fwrite($fp, $resultData);

if ($handle = opendir($path)) {
    echo "Verzeichnis-Handle:$handle\n";
    echo "Einträge:\n";
    $i = 0;
    while (false !== ($jsonFile = readdir($handle))) {
        if ($jsonFile != "." && $jsonFile != "..") {

            echo "$jsonFile\n";
            echo $path . "$jsonFile\n";
            $theFile = file_get_contents($path . $jsonFile);
            $content = json_decode($theFile);

            $i++;
            $connections = $content->connections;
            $duration = null;

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
                $durationStripped = 'Nix gefunden';
            }

            $bahnhofId = str_replace('.json', '', $jsonFile);

            echo $didokArray[$bahnhofId]['BEZEICHNUNG_OFFIZIELL'] . ' -> Fastest Connection: ' . $duration . PHP_EOL;

            $didokArray[$bahnhofId]['Dauer'] = $durationStripped;
            $didokArray[$bahnhofId]['BHFID2'] = $bahnhofId;

            if (isset($didokArray[$bahnhofId])) {
                //$didokBahnhofWerte = implode(';', $didokArray[$bahnhofId]);
                $didokZeile = '';
                foreach ($spaltenArray as $spalte) {
                    $didokZeile .= '"' . $didokArray[$bahnhofId][$spalte] . '",';
                }
                //$didokBahnhofWerte = '"' . implode('","', $didokZeile) . '"';
            } else {
                $didokZeile = 'nicht in der liste gefunden';
            }

            $resultData = $didokZeile . PHP_EOL;
            fwrite($fp, $resultData);
            //echo $resultData;

        }
    }

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