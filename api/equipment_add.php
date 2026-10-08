<?php
require_once __DIR__ . '/api_helpers.php';
require_method('POST');
$db = api_db();

$deviceTypeId = (int)post_string('device_type_id');
$manufacturerId = (int)post_string('manufacturer_id');
$serialNumber = strtoupper(post_string('serial_number'));

if (!valid_positive_id($deviceTypeId) || !valid_positive_id($manufacturerId)) {
    api_response(false, 'Please provide a valid active device type and manufacturer.', null, 400);
}
if (!valid_serial($serialNumber)) {
    api_response(false, 'Invalid serial number. Serial number must start with SN- and contain only alphanumeric characters after it.', null, 400);
}
if (!active_device_type_exists($db, $deviceTypeId)) {
    api_response(false, 'Selected device type is not active or does not exist.', null, 400);
}
if (!active_manufacturer_exists($db, $manufacturerId)) {
    api_response(false, 'Selected manufacturer is not active or does not exist.', null, 400);
}
if (fetch_one($db, 'SELECT id FROM equipment WHERE serial_number=?', 's', [$serialNumber])) {
    api_response(false, 'Serial number already exists.', null, 409);
}

$stmt = $db->prepare("INSERT INTO equipment (device_type_id, manufacturer_id, serial_number, status) VALUES (?, ?, ?, 'active')");
$stmt->bind_param('iis', $deviceTypeId, $manufacturerId, $serialNumber);
if (!$stmt->execute()) {
    api_response(false, 'Failed to add equipment.', null, 500);
}
$newId = $stmt->insert_id;
$stmt->close();
api_response(true, 'Equipment added successfully.', ['id' => $newId], 201);

