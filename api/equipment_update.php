<?php
require_once __DIR__ . '/api_helpers.php';
require_method('POST');
$db = api_db();

$id = (int)post_string('id');
$deviceTypeId = (int)post_string('device_type_id');
$manufacturerId = (int)post_string('manufacturer_id');
$serialNumber = strtoupper(post_string('serial_number'));
$status = post_string('status');

if (!valid_positive_id($id)) {
    api_response(false, 'Invalid equipment id.', null, 400);
}
if (!equipment_exists($db, $id)) {
    api_response(false, 'Equipment entry not found.', null, 404);
}
if (!valid_positive_id($deviceTypeId) || !active_device_type_exists($db, $deviceTypeId)) {
    api_response(false, 'Please provide a valid active device type.', null, 400);
}
if (!valid_positive_id($manufacturerId) || !active_manufacturer_exists($db, $manufacturerId)) {
    api_response(false, 'Please provide a valid active manufacturer.', null, 400);
}
if (!valid_serial($serialNumber)) {
    api_response(false, 'Invalid serial number. Serial number must start with SN- and contain only alphanumeric characters after it.', null, 400);
}
if (!valid_status_value($status)) {
    api_response(false, 'Invalid status. Status must be active or inactive.', null, 400);
}
if (fetch_one($db, 'SELECT id FROM equipment WHERE serial_number=? AND id<>?', 'si', [$serialNumber, $id])) {
    api_response(false, 'That serial number already belongs to another equipment record.', null, 409);
}

$stmt = $db->prepare('UPDATE equipment SET device_type_id=?, manufacturer_id=?, serial_number=?, status=? WHERE id=?');
$stmt->bind_param('iissi', $deviceTypeId, $manufacturerId, $serialNumber, $status, $id);
if (!$stmt->execute()) {
    api_response(false, 'Failed to update equipment.', null, 500);
}
$stmt->close();
api_response(true, 'Equipment updated successfully.', ['id' => $id]);

