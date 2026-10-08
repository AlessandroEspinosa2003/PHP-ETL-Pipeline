<?php
declare(strict_types=1);
include("functions.php");

function bind_dynamic_params($stmt, $types, $params)
{
     if ($types === '') {
          return;
     }

     $refs = [];
     foreach ($params as $i => $value) {
          $refs[$i] = $value;
     }

     $bindArgs = [$types];
     foreach ($refs as $i => &$ref) {
          $bindArgs[] = &$ref;
     }

     call_user_func_array([$stmt, 'bind_param'], $bindArgs);
}

$dblink = db_connect("equipment_prod");

$deviceTypes = fetch_active_device_types($dblink);
$manufacturers = fetch_active_manufacturers($dblink);

$results = [];
$errorMsg = "";

$searchMode = $_GET['search_mode'] ?? '';
$statusFilter = $_GET['status_filter'] ?? 'active';
$selectedDeviceType = $_GET['device_type_id'] ?? '';
$selectedManufacturer = $_GET['manufacturer_id'] ?? '';
$serialNumber = trim($_GET['serial_number'] ?? '');
$page = max(1, (int)($_GET['page'] ?? 1));

$perPage = 25;
$offset = 0;
$totalRows = 0;
$totalPages = 1;

if (isset($_GET['submit'])) {
     $baseSelectSql = "
        SELECT
            e.id,
            dt.name AS device_type,
            m.name AS manufacturer,
            e.serial_number,
            e.status
        FROM equipment e
        JOIN device_types dt ON e.device_type_id = dt.id
        JOIN manufacturers m ON e.manufacturer_id = m.id
    ";

     $baseCountSql = "
          SELECT COUNT(*) AS total
          FROM equipment e
          JOIN device_types dt ON e.device_type_id = dt.id
          JOIN manufacturers m ON e.manufacturer_id = m.id
     ";

     $whereClauses = [];
     $paramTypes = '';
     $paramValues = [];

    if ($searchMode === 'device_type') {
        if ($selectedDeviceType === '' || $selectedDeviceType === 'all') {
            $errorMsg = "Please select a specific active device type.";
        } else {
               $whereClauses[] = "e.status='active'";
               $whereClauses[] = "dt.status='active'";
               $whereClauses[] = "m.status='active'";
               $whereClauses[] = "dt.id=?";
               $paramTypes .= 'i';
               $paramValues[] = (int)$selectedDeviceType;

            if ($selectedManufacturer === '' || $selectedManufacturer === 'all') {
                    // No extra filter.
            } else {
                    $whereClauses[] = "m.id=?";
                    $paramTypes .= 'i';
                    $paramValues[] = (int)$selectedManufacturer;
            }
        }
    } elseif ($searchMode === 'manufacturer') {
        if ($selectedManufacturer === '' || $selectedManufacturer === 'all') {
            $errorMsg = "Please select a specific active manufacturer.";
        } else {
               $whereClauses[] = "e.status='active'";
               $whereClauses[] = "dt.status='active'";
               $whereClauses[] = "m.status='active'";
               $whereClauses[] = "m.id=?";
               $paramTypes .= 'i';
               $paramValues[] = (int)$selectedManufacturer;

            if ($selectedDeviceType === '' || $selectedDeviceType === 'all') {
                    // No extra filter.
            } else {
                    $whereClauses[] = "dt.id=?";
                    $paramTypes .= 'i';
                    $paramValues[] = (int)$selectedDeviceType;
            }
        }
    } elseif ($searchMode === 'serial_number') {
        if ($serialNumber === '') {
            $errorMsg = "Please enter a serial number.";
        } else {
               $whereClauses[] = "e.status='active'";
               $whereClauses[] = "e.serial_number=?";
               $paramTypes .= 's';
               $paramValues[] = $serialNumber;
        }
    } elseif ($searchMode === 'search_all') {
        if ($statusFilter === 'active') {
               $whereClauses[] = "e.status='active'";
        } elseif ($statusFilter === 'inactive') {
               $whereClauses[] = "e.status='inactive'";
        } else {
               // Show all statuses.
        }
    } else {
        $errorMsg = "Please select a search mode.";
    }

     if ($errorMsg === '') {
          $whereSql = '';
          if (count($whereClauses) > 0) {
               $whereSql = ' WHERE ' . implode(' AND ', $whereClauses);
          }

          $countStmt = $dblink->prepare($baseCountSql . $whereSql);
          bind_dynamic_params($countStmt, $paramTypes, $paramValues);
          $countStmt->execute();
          $countResult = $countStmt->get_result();
          $countRow = $countResult->fetch_assoc();
          $totalRows = (int)($countRow['total'] ?? 0);
          $countStmt->close();

          $totalPages = max(1, (int)ceil($totalRows / $perPage));
          if ($page > $totalPages) {
               $page = $totalPages;
          }
          $offset = ($page - 1) * $perPage;

          $dataSql = $baseSelectSql . $whereSql . " ORDER BY e.id LIMIT ? OFFSET ?";
          $dataStmt = $dblink->prepare($dataSql);
          $dataParamTypes = $paramTypes . 'ii';
          $dataParamValues = $paramValues;
          $dataParamValues[] = $perPage;
          $dataParamValues[] = $offset;

          bind_dynamic_params($dataStmt, $dataParamTypes, $dataParamValues);
          $dataStmt->execute();
          $queryResult = $dataStmt->get_result();
          while ($row = $queryResult->fetch_assoc()) {
               $results[] = $row;
          }
          $dataStmt->close();
     }
}
?>
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Advanced Software Engineering</title>
<link href="assets/css/bootstrap.css" rel="stylesheet">
<link rel="stylesheet" href="assets/css/font-awesome.min.css">
<link rel="stylesheet" href="assets/css/owl.carousel.css">
<link rel="stylesheet" href="assets/css/owl.theme.default.min.css">
<link rel="stylesheet" href="assets/css/templatemo-style.css">
</head>
<body id="top" data-spy="scroll" data-target=".navbar-collapse" data-offset="50">
     <section class="navbar custom-navbar navbar-fixed-top" role="navigation">
          <div class="container">
               <div class="navbar-header">
                    <button class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
                         <span class="icon icon-bar"></span>
                         <span class="icon icon-bar"></span>
                         <span class="icon icon-bar"></span>
                    </button>
                    <a href="#" class="navbar-brand">Search Equipment Database</a>
               </div>
               <div class="collapse navbar-collapse">
                    <ul class="nav navbar-nav navbar-nav-first">
                         <li><a href="index.php" class="smoothScroll">Home</a></li>
                         <li><a href="search.php" class="smoothScroll">Search Equipment</a></li>
                         <li><a href="add.php" class="smoothScroll">Add Equipment</a></li>
                    </ul>
               </div>
          </div>
     </section>

     <section id="home"></section>

     <section id="feature">
          <div class="container" style="margin-top:100px;">
               <div class="row">
                    <div class="col-md-12">
                         <div class="feature-thumb">
                              <h3>Search Equipment</h3>

                              <?php if ($errorMsg !== ''): ?>
                                   <div class="alert alert-danger" role="alert"><?php echo h($errorMsg); ?></div>
                              <?php endif; ?>

                              <form method="get" action="search.php">
                                   <div class="form-group">
                                        <label for="search_mode">Search Mode:</label>
                                        <select class="form-control" name="search_mode" id="search_mode" required>
                                             <option value="">Select Search Type</option>
                                             <option value="device_type" <?php if ($searchMode === 'device_type') echo 'selected'; ?>>By Device Type</option>
                                             <option value="manufacturer" <?php if ($searchMode === 'manufacturer') echo 'selected'; ?>>By Manufacturer</option>
                                             <option value="serial_number" <?php if ($searchMode === 'serial_number') echo 'selected'; ?>>By Serial Number</option>
                                             <option value="search_all" <?php if ($searchMode === 'search_all') echo 'selected'; ?>>Search All</option>
                                        </select>
                                   </div>

                                   <div class="form-group">
                                        <label for="device_type_id">Device Type:</label>
                                        <select class="form-control" name="device_type_id" id="device_type_id">
                                             <option value="">All / Select Device Type</option>
                                             <option value="all" <?php if ($selectedDeviceType === 'all') echo 'selected'; ?>>All Active Device Types</option>
                                             <?php foreach ($deviceTypes as $device): ?>
                                                  <option value="<?php echo (int)$device['id']; ?>" <?php if ((string)$selectedDeviceType === (string)$device['id']) echo 'selected'; ?>>
                                                       <?php echo h($device['name']); ?>
                                                  </option>
                                             <?php endforeach; ?>
                                        </select>
                                   </div>

                                   <div class="form-group">
                                        <label for="manufacturer_id">Manufacturer:</label>
                                        <select class="form-control" name="manufacturer_id" id="manufacturer_id">
                                             <option value="">All / Select Manufacturer</option>
                                             <option value="all" <?php if ($selectedManufacturer === 'all') echo 'selected'; ?>>All Active Manufacturers</option>
                                             <?php foreach ($manufacturers as $manufacturer): ?>
                                                  <option value="<?php echo (int)$manufacturer['id']; ?>" <?php if ((string)$selectedManufacturer === (string)$manufacturer['id']) echo 'selected'; ?>>
                                                       <?php echo h($manufacturer['name']); ?>
                                                  </option>
                                             <?php endforeach; ?>
                                        </select>
                                   </div>

                                   <div class="form-group">
                                        <label for="serial_number">Serial Number:</label>
                                        <input type="text" class="form-control" name="serial_number" id="serial_number" value="<?php echo h($serialNumber); ?>">
                                   </div>

                                   <div class="form-group">
                                        <label for="status_filter">Search All Filter:</label>
                                        <select class="form-control" name="status_filter" id="status_filter">
                                             <option value="active" <?php if ($statusFilter === 'active') echo 'selected'; ?>>Only Active</option>
                                             <option value="inactive" <?php if ($statusFilter === 'inactive') echo 'selected'; ?>>Only Inactive</option>
                                             <option value="all" <?php if ($statusFilter === 'all') echo 'selected'; ?>>All</option>
                                        </select>
                                   </div>

                                   <button type="submit" name="submit" value="submit" class="btn btn-primary">Search</button>
                              </form>
                         </div>
                    </div>

                    <?php if (isset($_GET['submit'])): ?>
                    <div class="col-md-12">
                         <div class="feature-thumb">
                              <h3>Search Results</h3>
                              <?php
                                   $startRow = $totalRows > 0 ? ($offset + 1) : 0;
                                   $endRow = $totalRows > 0 ? ($offset + count($results)) : 0;
                              ?>
                              <p>Showing <?php echo h((string)$startRow); ?> to <?php echo h((string)$endRow); ?> of <?php echo h((string)$totalRows); ?> results.</p>
                              <style>
                                   .search-pagination {
                                        display: flex;
                                        flex-wrap: wrap;
                                        gap: 6px;
                                        padding-left: 0;
                                        margin: 20px 0 0;
                                        list-style: none;
                                   }
                                   .search-pagination > li > a,
                                   .search-pagination > li > span {
                                        display: inline-block;
                                        width: 42px;
                                        height: 42px;
                                        line-height: 40px;
                                        text-align: center;
                                        padding: 0;
                                        border: 1px solid #d7d7d7;
                                        border-radius: 0 !important;
                                        background: #fff;
                                        color: #337ab7;
                                        text-decoration: none;
                                        box-sizing: border-box;
                                   }
                                   .search-pagination > li.active > span {
                                        background: #337ab7;
                                        border-color: #337ab7;
                                        color: #fff;
                                   }
                                   .search-pagination > li.disabled > span {
                                        color: #999;
                                        cursor: default;
                                   }
                              </style>
                              <div class="table-responsive">
                                   <table class="table table-bordered table-striped">
                                        <thead>
                                             <tr>
                                                  <th>ID</th>
                                                  <th>Device Type</th>
                                                  <th>Manufacturer</th>
                                                  <th>Serial Number</th>
                                                  <th>Status</th>
                                                  <th>View</th>
                                             </tr>
                                        </thead>
                                        <tbody>
                                             <?php if (count($results) > 0): ?>
                                                  <?php foreach ($results as $row): ?>
                                                       <tr>
                                                            <td><?php echo h((string)$row['id']); ?></td>
                                                            <td><?php echo h($row['device_type']); ?></td>
                                                            <td><?php echo h($row['manufacturer']); ?></td>
                                                            <td><?php echo h($row['serial_number']); ?></td>
                                                            <td><?php echo h($row['status']); ?></td>
                                                            <td><a href="view.php?id=<?php echo urlencode((string)$row['id']); ?>" class="btn btn-info btn-sm">View</a></td>
                                                       </tr>
                                                  <?php endforeach; ?>
                                             <?php else: ?>
                                                  <tr>
                                                       <td colspan="6">No results found.</td>
                                                  </tr>
                                             <?php endif; ?>
                                        </tbody>
                                   </table>
                              </div>

                              <?php if ($totalPages > 1): ?>
                                   <nav aria-label="Search result pages">
                                        <ul class="search-pagination">
                                             <?php
                                                  $baseParams = $_GET;
                                                  $windowSize = 2;
                                                  $windowStart = max(1, $page - $windowSize);
                                                  $windowEnd = min($totalPages, $page + $windowSize);
                                             ?>

                                             <?php
                                                  $pageLinks = [];
                                                  $pageLinks[] = 1;

                                                  for ($i = $windowStart; $i <= $windowEnd; $i++) {
                                                       $pageLinks[] = $i;
                                                  }

                                                  $pageLinks[] = $totalPages;
                                                  $pageLinks = array_values(array_unique($pageLinks));
                                                  sort($pageLinks);

                                                  $previousPageNumber = null;
                                                  foreach ($pageLinks as $pageNumber):
                                                       if ($previousPageNumber !== null && $pageNumber - $previousPageNumber > 1):
                                             ?>
                                                            <li class="disabled"><span aria-hidden="true">...</span></li>
                                             <?php
                                                       endif;

                                                       $baseParams['page'] = $pageNumber;
                                             ?>
                                                       <li class="<?php echo $pageNumber === $page ? 'active' : ''; ?>">
                                                            <?php if ($pageNumber === $page): ?>
                                                                 <span><?php echo h((string)$pageNumber); ?></span>
                                                            <?php else: ?>
                                                                 <a href="search.php?<?php echo h(http_build_query($baseParams)); ?>"><?php echo h((string)$pageNumber); ?></a>
                                                            <?php endif; ?>
                                                       </li>
                                             <?php
                                                       $previousPageNumber = $pageNumber;
                                                  endforeach;
                                             ?>

                                        </ul>
                                   </nav>
                              <?php endif; ?>
                         </div>
                    </div>
                    <?php endif; ?>
               </div>
          </div>
     </section>
</body>
</html>
<?php $dblink->close(); ?>
