<?php


$uri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);

switch ($uri) {
    case '/api/device-types/active':
        require __DIR__ . '/device_types_active.php';
        break;

    case '/api/device-types/add':
        require __DIR__ . '/device_types_add.php';
        break;

    case '/api/device-types/update':
        require __DIR__ . '/device_types_update.php';
        break;

    case '/api/manufacturers/active':
        require __DIR__ . '/manufacturers_active.php';
        break;

    case '/api/manufacturers/add':
        require __DIR__ . '/manufacturers_add.php';
        break;

    case '/api/manufacturers/update':
        require __DIR__ . '/manufacturers_update.php';
        break;

    case '/api/equipment/active':
        require __DIR__ . '/equipment_active.php';
        break;

    case '/api/equipment/search':
        require __DIR__ . '/equipment_search.php';
        break;

    case '/api/equipment/add':
        require __DIR__ . '/equipment_add.php';
        break;

    case '/api/equipment/view':
        require __DIR__ . '/equipment_view.php';
        break;

    case '/api/equipment/update':
        require __DIR__ . '/equipment_update.php';
        break;

    default:
        http_response_code(404);
        echo json_encode([
            'success' => false,
            'message' => 'Endpoint not found.',
            'data' => null
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
        exit;
}
