<?php
require_once __DIR__ . '/api_helpers.php';
require_method('GET');
$db = api_db();
$rows = fetch_all_rows($db, "
    SELECT e.id, e.device_type_id, e.manufacturer_id, dt.name AS device_type, m.name AS manufacturer, e.serial_number, e.status
    FROM equipment e
    JOIN device_types dt ON e.device_type_id = dt.id
    JOIN manufacturers m ON e.manufacturer_id = m.id
    WHERE e.status='active'
    ORDER BY e.id
    LIMIT 1000
");
api_response(true, count($rows) > 0 ? 'Active equipment found. Results limited to 1000.' : 'No active equipment found.', $rows);

