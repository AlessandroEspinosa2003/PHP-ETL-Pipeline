<?php
require_once __DIR__ . '/api_helpers.php';
require_method('POST');
$db = api_db();
$name = post_string('name');

if ($name === '' || !valid_alpha_spaces($name)) {
    api_response(false, 'Invalid manufacturer name. Manufacturer may only contain alphabet letters and spaces.', null, 400);
}
if (fetch_one($db, 'SELECT id FROM manufacturers WHERE LOWER(name)=LOWER(?)', 's', [$name])) {
    api_response(false, 'Manufacturer already exists.', null, 409);
}
$stmt = $db->prepare("INSERT INTO manufacturers (name, status) VALUES (?, 'active')");
$stmt->bind_param('s', $name);
if (!$stmt->execute()) {
    api_response(false, 'Failed to add manufacturer.', null, 500);
}
$newId = $stmt->insert_id;
$stmt->close();
api_response(true, 'Manufacturer added successfully.', ['id' => $newId], 201);

