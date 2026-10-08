<?php
require_once __DIR__ . '/api_helpers.php';
require_method('GET');
$db = api_db();
$rows = fetch_all_rows($db, "SELECT id, name, status FROM device_types WHERE status='active' ORDER BY name");
api_response(true, count($rows) > 0 ? 'Active device types found.' : 'No active device types found.', $rows);

