<?php
require_once __DIR__ . '/api_helpers.php';
require_method('POST');
$db = api_db();

$id = (int)post_string('id');
$name = post_string('name');
$status = post_string('status');

if (!valid_positive_id($id)) {
    api_response(false, 'Invalid manufacturer id.', null, 400);
}
if (!fetch_one($db, 'SELECT id FROM manufacturers WHERE id=?', 'i', [$id])) {
    api_response(false, 'Manufacturer not found.', null, 404);
}
if ($name === '' || !valid_alpha_spaces($name)) {
    api_response(false, 'Invalid manufacturer name. Manufacturer may only contain alphabet letters and spaces.', null, 400);
}
if (!valid_status_value($status)) {
    api_response(false, 'Invalid status. Status must be active or inactive.', null, 400);
}
if (fetch_one($db, 'SELECT id FROM manufacturers WHERE LOWER(name)=LOWER(?) AND id<>?', 'si', [$name, $id])) {
    api_response(false, 'Manufacturer name already exists.', null, 409);
}
$stmt = $db->prepare('UPDATE manufacturers SET name=?, status=? WHERE id=?');
$stmt->bind_param('ssi', $name, $status, $id);
if (!$stmt->execute()) {
    api_response(false, 'Failed to update manufacturer.', null, 500);
}
$stmt->close();
api_response(true, 'Manufacturer updated successfully.', ['id' => $id]);

