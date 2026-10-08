<?php

function db_connect($dbName){ 
 	$un="webuser";
 	$pw="iwCHEWoWnECHEw9!";
 	$db=$dbName;
 	$hostname="localhost";
 	$dblink=new mysqli($hostname,$un,$pw,$db);
 	return $dblink;
}


function log_activity($endPoint,$remoteClient,$parameters){ 
 	$date=date("Y-m-d hh:mm:ss");
}

function log_error($endPoint,$remoteClient){
 	$date=date("Y-m-d hh:mm:ss");
}

function redirect($url){
    header("Location: " . $url);
    exit;
}

function h($value){
    return htmlspecialchars((string)$value, ENT_QUOTES, 'UTF-8');
}

function valid_serial($serial){
    return (bool)preg_match('/^SN-[A-Za-z0-9]+$/', $serial);
}

function valid_alpha_spaces($value){
    return (bool)preg_match('/^[A-Za-z ]+$/', $value);
}

function fetch_active_device_types($db){
    $rows = [];
    $sql = "SELECT id, name FROM device_types WHERE status='active' ORDER BY name";
    $result = $db->query($sql);
    while ($row = $result->fetch_assoc()) {
        $rows[] = $row;
    }
    return $rows;
}

function fetch_active_manufacturers($db){
    $rows = [];
    $sql = "SELECT id, name FROM manufacturers WHERE status='active' ORDER BY name";
    $result = $db->query($sql);
    while ($row = $result->fetch_assoc()) {
        $rows[] = $row;
    }
    return $rows;
}
?>
