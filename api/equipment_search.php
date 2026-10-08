<?php
require_once __DIR__ . '/api_helpers.php';
require_method('GET');
$db = api_db();

$mode = get_string('mode');
$status = get_string('status') ?: 'active';
$deviceTypeId = get_string('device_type_id');
$manufacturerId = get_string('manufacturer_id');
$serialNumber = strtoupper(get_string('serial_number'));

if (!in_array($mode, ['device_type', 'manufacturer', 'serial_number', 'all'], true)) {
    api_response(false, 'Invalid search data. Search mode must be device_type, manufacturer, serial_number, or all.', null, 400);
}
if ($status !== 'all' && !valid_status_value($status)) {
    api_response(false, 'Invalid search data. Status must be active, inactive, or all.', null, 400);
}

$sql = "
    SELECT e.id, e.device_type_id, e.manufacturer_id, dt.name AS device_type, m.name AS manufacturer, e.serial_number, e.status
    FROM equipment e
    JOIN device_types dt ON e.device_type_id = dt.id
    JOIN manufacturers m ON e.manufacturer_id = m.id
";
$where = [];
$types = '';
$params = [];

if ($status !== 'all') {
    $where[] = 'e.status=?';
    $types .= 's';
    $params[] = $status;
}

if ($mode === 'device_type') {
    if (!valid_positive_id($deviceTypeId)) {
        api_response(false, 'Invalid search data. Please provide a valid device_type_id.', null, 400);
    }
    $where[] = 'e.device_type_id=?';
    $types .= 'i';
    $params[] = (int)$deviceTypeId;

    if ($manufacturerId !== '' && $manufacturerId !== 'all') {
        if (!valid_positive_id($manufacturerId)) {
            api_response(false, 'Invalid search data. Manufacturer must be a valid id or all.', null, 400);
        }
        $where[] = 'e.manufacturer_id=?';
        $types .= 'i';
        $params[] = (int)$manufacturerId;
    }
} elseif ($mode === 'manufacturer') {
    if (!valid_positive_id($manufacturerId)) {
        api_response(false, 'Invalid search data. Please provide a valid manufacturer_id.', null, 400);
    }
    $where[] = 'e.manufacturer_id=?';
    $types .= 'i';
    $params[] = (int)$manufacturerId;

    if ($deviceTypeId !== '' && $deviceTypeId !== 'all') {
        if (!valid_positive_id($deviceTypeId)) {
            api_response(false, 'Invalid search data. Device type must be a valid id or all.', null, 400);
        }
        $where[] = 'e.device_type_id=?';
        $types .= 'i';
        $params[] = (int)$deviceTypeId;
    }
} elseif ($mode === 'serial_number') {
    if ($serialNumber === '' || !valid_serial($serialNumber)) {
        api_response(false, 'Invalid search data. Please provide a valid serial number.', null, 400);
    }
    $where[] = 'e.serial_number=?';
    $types .= 's';
    $params[] = $serialNumber;
}

if (count($where) > 0) {
    $sql .= ' WHERE ' . implode(' AND ', $where);
}
$sql .= ' ORDER BY e.id LIMIT 1000';

$rows = fetch_all_rows($db, $sql, $types, $params);
api_response(true, count($rows) > 0 ? 'Search completed successfully. Results limited to 1000.' : 'No results found.', [
    'count' => count($rows),
    'limit' => 1000,
    'results' => $rows
]);

