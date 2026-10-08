<?php
require_once __DIR__ . '/api_helpers.php';
require_method('GET');
$db = api_db();
$id = get_string('id');

if (!valid_positive_id($id)) {
    api_response(false, 'Invalid view criteria. Please provide a valid equipment id.', null, 400);
}
$row = fetch_one($db, "
    SELECT e.id, e.device_type_id, e.manufacturer_id, dt.name AS device_type, dt.status AS device_type_status, m.name AS manufacturer, m.status AS manufacturer_status, e.serial_number, e.status
    FROM equipment e
    JOIN device_types dt ON e.device_type_id = dt.id
    JOIN manufacturers m ON e.manufacturer_id = m.id
    WHERE e.id=?
", 'i', [(int)$id]);

if (!$row) {
    api_response(false, 'Equipment entry not found.', null, 404);
}
api_response(true, 'Equipment entry found.', $row);

