<?php
require_once __DIR__ . '/../web/functions.php';

function api_db() {
    $db = db_connect('equipment_prod');
    if ($db->connect_error) {
        api_response(false, 'Database connection failed.', null, 500);
    }
    $db->set_charset('utf8mb4');
    return $db;
}

function api_response($success, $message, $data = null, $statusCode = 200) {
    http_response_code($statusCode);
    header('Content-Type: application/json; charset=utf-8');
    header('Access-Control-Allow-Origin: *');
    header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
    header('Access-Control-Allow-Headers: Content-Type');

    echo json_encode([
        'success' => $success,
        'message' => $message,
        'data' => $data
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
    exit;
}

function api_options_exit() {
    if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
        api_response(true, 'Preflight OK.', []);
    }
}

function require_method($method) {
    api_options_exit();
    if ($_SERVER['REQUEST_METHOD'] !== strtoupper($method)) {
        api_response(false, 'Invalid request method. Use ' . strtoupper($method) . '.', null, 405);
    }
}

function post_string($key) {
    return trim((string)($_POST[$key] ?? ''));
}

function get_string($key) {
    return trim((string)($_GET[$key] ?? ''));
}

function valid_positive_id($value) {
    return filter_var($value, FILTER_VALIDATE_INT) !== false && (int)$value > 0;
}

function valid_status_value($status) {
    return in_array($status, ['active', 'inactive'], true);
}

function fetch_one($db, $sql, $types = '', $params = []) {
    $stmt = $db->prepare($sql);
    if (!$stmt) {
        api_response(false, 'Failed to prepare database query.', null, 500);
    }
    if ($types !== '') {
        $stmt->bind_param($types, ...$params);
    }
    $stmt->execute();
    $result = $stmt->get_result();
    $row = $result->fetch_assoc();
    $stmt->close();
    return $row ?: null;
}

function fetch_all_rows($db, $sql, $types = '', $params = []) {
    $stmt = $db->prepare($sql);
    if (!$stmt) {
        api_response(false, 'Failed to prepare database query.', null, 500);
    }
    if ($types !== '') {
        $stmt->bind_param($types, ...$params);
    }
    $stmt->execute();
    $result = $stmt->get_result();
    $rows = [];
    while ($row = $result->fetch_assoc()) {
        $rows[] = $row;
    }
    $stmt->close();
    return $rows;
}

function active_device_type_exists($db, $id) {
    return fetch_one($db, 'SELECT id FROM device_types WHERE id=? AND status=\'active\'', 'i', [$id]) !== null;
}

function active_manufacturer_exists($db, $id) {
    return fetch_one($db, 'SELECT id FROM manufacturers WHERE id=? AND status=\'active\'', 'i', [$id]) !== null;
}

function equipment_exists($db, $id) {
    return fetch_one($db, 'SELECT id FROM equipment WHERE id=?', 'i', [$id]) !== null;
}
