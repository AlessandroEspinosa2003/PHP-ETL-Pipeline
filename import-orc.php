<?php

    $dir = "/var/www/html/parts_prod";
    $dirArray = array_diff(scandir($dir), array('..', '.'));
    natsort($dirArray);
    $dirArray = array_values($dirArray);

    foreach ($dirArray as $key => $value) {
      echo "Found file: $value with key: $key \r\n";
      shell_exec("/usr/bin/php /var/www/html/import.php $key $value > /var/www/html/logs/$value.log 2>/var/www/html/logs/$value.log");
      sleep(2);
    }

    echo "Orchestrator Done \n";
?>
