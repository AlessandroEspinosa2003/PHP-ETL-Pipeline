<?php
declare(strict_types=1);

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

function buildPlaceholders(int $rows, int $cols): string
{
    $one = '(' . implode(',', array_fill(0, $cols, '?')) . ')';
    return implode(',', array_fill(0, $rows, $one));
}

function flattenParams(array $rows, array $keys): array
{
    $flat = [];
    foreach ($rows as $row) {
        foreach ($keys as $key) {
            $flat[] = $row[$key];
        }
    }
    return $flat;
}

function flushDirtyBatch(mysqli $db, array &$dirtyBatch): void
{
    if (empty($dirtyBatch)) {
        return;
    }

    $sql = "
        INSERT INTO dirty_devices
        (
            source_line, raw_line, parsed_device_type, parsed_manufacturer,
            parsed_serial_number, column_count, error_code, error_detail
        )
        VALUES " . buildPlaceholders(count($dirtyBatch), 8);

    $stmt = $db->prepare($sql);
    $types = str_repeat('issssiss', count($dirtyBatch));
    $params = flattenParams($dirtyBatch, [
        'source_line',
        'raw_line',
        'parsed_device_type',
        'parsed_manufacturer',
        'parsed_serial_number',
        'column_count',
        'error_code',
        'error_detail',
    ]);

    $stmt->bind_param($types, ...$params);
    $stmt->execute();
    $stmt->close();

    $dirtyBatch = [];
}

function flushRowErrorBatch(mysqli $db, array &$rowErrorBatch): void
{
    if (empty($rowErrorBatch)) {
        return;
    }

    $sql = "
        INSERT INTO row_errors
        (source_line, error_code, error_detail, raw_line)
        VALUES " . buildPlaceholders(count($rowErrorBatch), 4);

    $stmt = $db->prepare($sql);
    $types = str_repeat('isss', count($rowErrorBatch));
    $params = flattenParams($rowErrorBatch, [
        'source_line',
        'error_code',
        'error_detail',
        'raw_line',
    ]);

    $stmt->bind_param($types, ...$params);
    $stmt->execute();
    $stmt->close();

    $rowErrorBatch = [];
}

function flushDuplicateBatch(mysqli $db, array &$duplicateBatch): void
{
    if (empty($duplicateBatch)) {
        return;
    }

    $sql = "
        INSERT INTO duplicate_devices
        (
            source_line, raw_line, device_type, manufacturer,
            serial_number, duplicate_reason, original_clean_id
        )
        VALUES " . buildPlaceholders(count($duplicateBatch), 7);

    $stmt = $db->prepare($sql);
    $types = str_repeat('isssssi', count($duplicateBatch));
    $params = flattenParams($duplicateBatch, [
        'source_line',
        'raw_line',
        'device_type',
        'manufacturer',
        'serial_number',
        'duplicate_reason',
        'original_clean_id',
    ]);

    $stmt->bind_param($types, ...$params);
    $stmt->execute();
    $stmt->close();

    $duplicateBatch = [];
}

$key  = (int)$argv[1];
$file = $argv[2] ?? '';

if ($file === '') {
    die("Usage: php import.php <chunk_key> <filename>\n");
}

$un = "USERNAME";
$pw = "PASSWORD";
$host = "localhost";
$db = "equipment";
$logDB = "equipment_error_logs";

$dblink = new mysqli($host, $un, $pw, $db);
$logDBlink = new mysqli($host, $un, $pw, $logDB);

$dblink->set_charset("utf8mb4");
$logDBlink->set_charset("utf8mb4");

$fp = fopen("parts_prod/$file", "r");
if ($fp === false) {
    die("Could not open parts_prod/$file\n");
}

echo "Working on file: $file\r\n";

$start = microtime(true);
echo "Start Time: $start\r\n";

$localLine = 0;
$linesPerPart = 500000;
$baseLine = $key * $linesPerPart;
$count = 0;
$batchSize = 250;

$err_missing_device_count = 0;
$err_missing_manufacturer_count = 0;
$err_missing_serial_count = 0;
$err_too_many_count = 0;
$err_blank_count = 0;
$err_too_many_and_blank_count = 0;
$err_invalid_manufacturer_count = 0;
$err_invalid_device_count = 0;
$err_invalid_serial_count = 0;
$multi_errors_count = 0;
$leading_comma_count = 0;
$trailing_comma_count = 0;

$valid_rows_count = 0;
$dirty_rows_count = 0;
$duplicate_rows_count = 0;
$exactly_three_cols_count = 0;
$too_few_cols_count = 0;

$dirtyBatch = [];
$duplicateBatch = [];
$rowErrorBatch = [];

$valid_manufacturers = [
    "Sony" => true,
    "Ford" => true,
    "Vizio" => true,
    "LG" => true,
    "Huawei" => true,
    "Google" => true,
    "Apple" => true,
    "IBM" => true,
    "Samsung" => true,
    "Dell" => true,
    "Microsoft" => true,
    "Nissan" => true,
    "HP" => true,
    "Panasonic" => true,
    "GM" => true,
    "KIA" => true,
    "Toyota" => true,
    "Chevorlet" => true,
    "TCL" => true,
    "OnePlus" => true,
    "Hisense" => true,
    "Hyundai" => true,
    "Motorola" => true,
    "Nokia" => true
];

$valid_device_types = [
    "computer" => true,
    "laptop" => true,
    "tablet" => true,
    "smart watch"  => true,
    "mobile phone" => true,
    "television" => true,
    "vehicle" => true
];

$insertCleanStmt = $dblink->prepare("
    INSERT INTO clean_devices
    (source_line, device_type, manufacturer, serial_number)
    VALUES (?, ?, ?, ?)
");

$findCleanBySerialStmt = $dblink->prepare("
    SELECT id
    FROM clean_devices
    WHERE serial_number = ?
    LIMIT 1
");

try {
    $dblink->begin_transaction();
    $logDBlink->begin_transaction();

    while (($raw = fgets($fp)) !== false) {
        $localLine++;
        $globalLine = $baseLine + $localLine;
        $count++;
        $errors = [];

        $rawTrim = rtrim($raw, "\r\n");

        $line = str_getcsv($rawTrim, ',', '"', "\\");
        $cols = count($line);

        if ($cols > 3) {
            $allBlank = true;
            foreach ($line as $fld) {
                if (trim($fld) !== '') {
                    $allBlank = false;
                    break;
                }
            }

            $err_too_many_count++;

            if ($allBlank) {
                $err_too_many_and_blank_count++;
                $errors[] = "BLANK_LINE_TOO_MANY_COLUMNS";
            } else {
                $errors[] = "TOO_MANY_COLUMNS";
            }
        } elseif ($cols === 3) {
            $exactly_three_cols_count++;
        } elseif ($cols < 3) {
            $errors[] = "TOO_FEW_COLUMNS";
            $too_few_cols_count++;
        }

        $deviceRaw = '';
        $manufacturerRaw = '';
        $serialRaw = '';

        if ($cols === 3) {
            $deviceRaw       = $line[0] ?? '';
            $manufacturerRaw = $line[1] ?? '';
            $serialRaw       = $line[2] ?? '';

        } elseif ($cols === 4 && $rawTrim !== '' && $rawTrim[0] === ',') {
            $deviceRaw       = $line[1] ?? '';
            $manufacturerRaw = $line[2] ?? '';
            $serialRaw       = $line[3] ?? '';

        } elseif ($cols === 4 && $rawTrim !== '' && substr($rawTrim, -1) === ',') {
            $deviceRaw       = $line[0] ?? '';
            $manufacturerRaw = $line[1] ?? '';
            $serialRaw       = $line[2] ?? '';

        } else {
            $deviceRaw       = $line[0] ?? '';
            $manufacturerRaw = $line[1] ?? '';
            $serialRaw       = $line[2] ?? '';
        }

        $device       = strtolower(trim($deviceRaw));
        $manufacturer = trim($manufacturerRaw);
        $serial       = trim($serialRaw);

        if ($device === '' && $manufacturer === '' && $serial === '') {
            $err_blank_count++;
            $errors[] = "BLANK_LINE";
        } else {
            if ($device === '') {
                $err_missing_device_count++;
                $errors[] = "MISSING_DEVICE_TYPE";
            }

            if ($manufacturer === '') {
                $err_missing_manufacturer_count++;
                $errors[] = "MISSING_MANUFACTURER";
            }

            if ($serial === '') {
                $err_missing_serial_count++;
                $errors[] = "MISSING_SERIAL_NUMBER";
            }
        }

        if ($device !== '' && !isset($valid_device_types[$device])) {
            $err_invalid_device_count++;
            $errors[] = "INVALID_DEVICE_TYPE";
        }

        if ($manufacturer !== '' && !isset($valid_manufacturers[$manufacturer])) {
            $err_invalid_manufacturer_count++;
            $errors[] = "INVALID_MANUFACTURER";
        }

        if ($serial !== '' && !preg_match('/^SN-[a-f0-9]{64}$/', $serial)) {
            $err_invalid_serial_count++;
            $errors[] = "INVALID_SERIAL_NUMBER";
        }

        if ($rawTrim !== '' && $rawTrim[0] === ',') {
            $leading_comma_count++;
            $errors[] = "LEADING_COMMA";
        }

        if ($rawTrim !== '' && substr($rawTrim, -1) === ',') {
            $trailing_comma_count++;
            $errors[] = "TRAILING_COMMA";
        }

        if (count($errors) > 1) {
            $multi_errors_count++;
        }

        if (!empty($errors)) {
            $dirty_rows_count++;
            $primaryErrorCode = $errors[0];
            $errorDetail = implode(' | ', $errors);

            $dirtyBatch[] = [
                'source_line' => $globalLine,
                'raw_line' => $rawTrim,
                'parsed_device_type' => $device,
                'parsed_manufacturer' => $manufacturer,
                'parsed_serial_number' => $serial,
                'column_count' => $cols,
                'error_code' => $primaryErrorCode,
                'error_detail' => $errorDetail,
            ];

            foreach ($errors as $errMsg) {
                $rowErrorBatch[] = [
                    'source_line' => $globalLine,
                    'error_code' => $errMsg,
                    'error_detail' => $errMsg,
                    'raw_line' => $rawTrim,
                ];
            }
        } else {
            try {
                $insertCleanStmt->bind_param('isss', $globalLine, $device, $manufacturer, $serial);
                $insertCleanStmt->execute();
                $valid_rows_count++;
            } catch (mysqli_sql_exception $e) {
                if ((int)$e->getCode() === 1062) {
                    $findCleanBySerialStmt->bind_param('s', $serial);
                    $findCleanBySerialStmt->execute();
                    $result = $findCleanBySerialStmt->get_result();

                    if ($result && ($row = $result->fetch_assoc())) {
                        $duplicate_rows_count++;
                        $duplicateBatch[] = [
                            'source_line' => $globalLine,
                            'raw_line' => $rawTrim,
                            'device_type' => $device,
                            'manufacturer' => $manufacturer,
                            'serial_number' => $serial,
                            'duplicate_reason' => 'DUPLICATE_SERIAL',
                            'original_clean_id' => (int)$row['id'],
                        ];

                        $rowErrorBatch[] = [
                            'source_line' => $globalLine,
                            'error_code' => 'DUPLICATE_SERIAL',
                            'error_detail' => 'DUPLICATE_SERIAL_NUMBER_REJECTED_FROM_CLEAN_DEVICES',
                            'raw_line' => $rawTrim,
                        ];
                    } else {
                        $dirty_rows_count++;
                        $dirtyBatch[] = [
                            'source_line' => $globalLine,
                            'raw_line' => $rawTrim,
                            'parsed_device_type' => $device,
                            'parsed_manufacturer' => $manufacturer,
                            'parsed_serial_number' => $serial,
                            'column_count' => $cols,
                            'error_code' => 'DUPLICATE_SOURCE_LINE',
                            'error_detail' => $e->getMessage(),
                        ];

                        $rowErrorBatch[] = [
                            'source_line' => $globalLine,
                            'error_code' => 'DUPLICATE_SOURCE_LINE',
                            'error_detail' => $e->getMessage(),
                            'raw_line' => $rawTrim,
                        ];
                    }
                } else {
                    $dirty_rows_count++;
                    $dirtyBatch[] = [
                        'source_line' => $globalLine,
                        'raw_line' => $rawTrim,
                        'parsed_device_type' => $device,
                        'parsed_manufacturer' => $manufacturer,
                        'parsed_serial_number' => $serial,
                        'column_count' => $cols,
                        'error_code' => 'INSERT_ERROR',
                        'error_detail' => $e->getMessage(),
                    ];

                    $rowErrorBatch[] = [
                        'source_line' => $globalLine,
                        'error_code' => 'INSERT_ERROR',
                        'error_detail' => $e->getMessage(),
                        'raw_line' => $rawTrim,
                    ];
                }
            }
        }

        if (count($dirtyBatch) >= $batchSize) {
            flushDirtyBatch($dblink, $dirtyBatch);
        }

        if (count($duplicateBatch) >= $batchSize) {
            flushDuplicateBatch($dblink, $duplicateBatch);
        }

        if (count($rowErrorBatch) >= $batchSize) {
            flushRowErrorBatch($logDBlink, $rowErrorBatch);
        }
    }

    fclose($fp);

    flushDirtyBatch($dblink, $dirtyBatch);
    flushDuplicateBatch($dblink, $duplicateBatch);
    flushRowErrorBatch($logDBlink, $rowErrorBatch);

    $dblink->commit();
    $logDBlink->commit();

} catch (Throwable $e) {
    if (is_resource($fp)) {
        fclose($fp);
    }

    $dblink->rollback();
    $logDBlink->rollback();
    throw $e;
}

$insertCleanStmt->close();
$findCleanBySerialStmt->close();

$dblink->close();
$logDBlink->close();

$end = microtime(true);
$timeSeconds = $end - $start;
$timeMin = $timeSeconds / 60;
$rps = ($timeSeconds > 0) ? ($count / $timeSeconds) : 0;

echo "Complete\r\n";
echo "Time Seconds: $timeSeconds\r\n";
echo "Time Minutes: $timeMin\r\n";
echo "Rows per second: $rps\r\n";

echo "Rows Seen: $count\r\n";
echo "Valid Rows: $valid_rows_count\r\n";
echo "Dirty Rows: $dirty_rows_count\r\n";
echo "Duplicate Rows: $duplicate_rows_count\r\n";
echo "Exactly 3 Cols: $exactly_three_cols_count\r\n";
echo "Too Few Cols: $too_few_cols_count\r\n";

echo "Too many cols: $err_too_many_count\r\n";
echo "Blank Lines: $err_blank_count\r\n";
echo "Too many & blank: $err_too_many_and_blank_count\r\n";

echo "Missing Device: $err_missing_device_count\r\n";
echo "Missing Manufacturer: $err_missing_manufacturer_count\r\n";
echo "Missing Serial Number: $err_missing_serial_count\r\n";

echo "Invalid Manufacturer: $err_invalid_manufacturer_count\r\n";
echo "Invalid Device Type: $err_invalid_device_count\r\n";
echo "Invalid Serial Number: $err_invalid_serial_count\r\n";

echo "Leading Comma: $leading_comma_count\r\n";
echo "Trailing Comma: $trailing_comma_count\r\n";

echo "Multiple Errors: $multi_errors_count\r\n";
?>
