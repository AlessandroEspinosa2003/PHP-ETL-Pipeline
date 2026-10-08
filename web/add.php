<?php
include("functions.php");

$dblink = db_connect("equipment_prod");
$msg = $_GET['msg'] ?? '';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $action = $_POST['action'] ?? '';

    if ($action === 'add_equipment') {
        $deviceTypeId = (int)($_POST['device_type_id'] ?? 0);
        $manufacturerId = (int)($_POST['manufacturer_id'] ?? 0);
        $serialNumber = strtoupper(trim($_POST['serial_number'] ?? ''));

        if ($deviceTypeId <= 0 || $manufacturerId <= 0) {
            redirect("add.php?msg=InvalidSelection");
        }

        if (!valid_serial($serialNumber)) {
            redirect("add.php?msg=InvalidSerial");
        }

        $stmt = $dblink->prepare("SELECT id FROM device_types WHERE id=? AND status='active'");
        $stmt->bind_param("i", $deviceTypeId);
        $stmt->execute();
        $deviceExists = $stmt->get_result()->num_rows > 0;
        $stmt->close();

        $stmt = $dblink->prepare("SELECT id FROM manufacturers WHERE id=? AND status='active'");
        $stmt->bind_param("i", $manufacturerId);
        $stmt->execute();
        $manufacturerExists = $stmt->get_result()->num_rows > 0;
        $stmt->close();

        if (!$deviceExists || !$manufacturerExists) {
            redirect("add.php?msg=InvalidSelection");
        }

        $stmt = $dblink->prepare("SELECT id FROM equipment WHERE serial_number=?");
        $stmt->bind_param("s", $serialNumber);
        $stmt->execute();
        $serialExists = $stmt->get_result()->num_rows > 0;
        $stmt->close();

        if ($serialExists) {
            redirect("add.php?msg=DeviceExists");
        }

        $stmt = $dblink->prepare(
            "INSERT INTO equipment (device_type_id, manufacturer_id, serial_number, status)
             VALUES (?, ?, ?, 'active')"
        );
        $stmt->bind_param("iis", $deviceTypeId, $manufacturerId, $serialNumber);
        $stmt->execute();
        $stmt->close();

        redirect("index.php?msg=EquipmentAdded");
    }

    if ($action === 'add_device_type') {
        $name = trim($_POST['device_type_name'] ?? '');

        if ($name === '' || !valid_alpha_spaces($name)) {
            redirect("add.php?msg=InvalidDeviceType");
        }

        $stmt = $dblink->prepare("SELECT id FROM device_types WHERE LOWER(name)=LOWER(?)");
        $stmt->bind_param("s", $name);
        $stmt->execute();
        $exists = $stmt->get_result()->num_rows > 0;
        $stmt->close();

        if ($exists) {
            redirect("add.php?msg=DeviceTypeExists");
        }

        $stmt = $dblink->prepare("INSERT INTO device_types (name, status) VALUES (?, 'active')");
        $stmt->bind_param("s", $name);
        $stmt->execute();
        $stmt->close();

        redirect("index.php?msg=DeviceTypeAdded");
    }

    if ($action === 'add_manufacturer') {
        $name = trim($_POST['manufacturer_name'] ?? '');

        if ($name === '' || !valid_alpha_spaces($name)) {
            redirect("add.php?msg=InvalidManufacturer");
        }

        $stmt = $dblink->prepare("SELECT id FROM manufacturers WHERE LOWER(name)=LOWER(?)");
        $stmt->bind_param("s", $name);
        $stmt->execute();
        $exists = $stmt->get_result()->num_rows > 0;
        $stmt->close();

        if ($exists) {
            redirect("add.php?msg=ManufacturerExists");
        }

        $stmt = $dblink->prepare("INSERT INTO manufacturers (name, status) VALUES (?, 'active')");
        $stmt->bind_param("s", $name);
        $stmt->execute();
        $stmt->close();

        redirect("index.php?msg=ManufacturerAdded");
    }
}

$deviceTypes = fetch_active_device_types($dblink);
$manufacturers = fetch_active_manufacturers($dblink);
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
                    <a href="#" class="navbar-brand">Add New Equipment</a>
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
                         <?php if ($msg === "DeviceExists"): ?>
                              <div class="alert alert-danger" role="alert">Serial number already exists in the database.</div>
                         <?php elseif ($msg === "InvalidSerial"): ?>
                              <div class="alert alert-danger" role="alert">Serial number must start with SN- and contain only alphanumeric characters after it.</div>
                         <?php elseif ($msg === "InvalidSelection"): ?>
                              <div class="alert alert-danger" role="alert">Please choose an active device type and manufacturer.</div>
                         <?php elseif ($msg === "InvalidDeviceType"): ?>
                              <div class="alert alert-danger" role="alert">Device type may only contain alphabet letters and spaces.</div>
                         <?php elseif ($msg === "DeviceTypeExists"): ?>
                              <div class="alert alert-danger" role="alert">That device type already exists.</div>
                         <?php elseif ($msg === "InvalidManufacturer"): ?>
                              <div class="alert alert-danger" role="alert">Manufacturer may only contain alphabet letters and spaces.</div>
                         <?php elseif ($msg === "ManufacturerExists"): ?>
                              <div class="alert alert-danger" role="alert">That manufacturer already exists.</div>
                         <?php endif; ?>
                    </div>

                    <div class="col-md-6 col-sm-6">
                         <div class="feature-thumb">
                              <h3>Add Equipment</h3>
                              <form method="post" action="">
                                   <input type="hidden" name="action" value="add_equipment">

                                   <div class="form-group">
                                        <label for="device_type_id">Device Type:</label>
                                        <select class="form-control" name="device_type_id" id="device_type_id" required>
                                             <option value="">Select Active Device Type</option>
                                             <?php foreach ($deviceTypes as $device): ?>
                                                  <option value="<?php echo (int)$device['id']; ?>"><?php echo h($device['name']); ?></option>
                                             <?php endforeach; ?>
                                        </select>
                                   </div>

                                   <div class="form-group">
                                        <label for="manufacturer_id">Manufacturer:</label>
                                        <select class="form-control" name="manufacturer_id" id="manufacturer_id" required>
                                             <option value="">Select Active Manufacturer</option>
                                             <?php foreach ($manufacturers as $manufacturer): ?>
                                                  <option value="<?php echo (int)$manufacturer['id']; ?>"><?php echo h($manufacturer['name']); ?></option>
                                             <?php endforeach; ?>
                                        </select>
                                   </div>

                                   <div class="form-group">
                                        <label for="serial_number">Serial Number:</label>
                                        <input type="text" class="form-control" id="serial_number" name="serial_number" placeholder="SN-ABC123" required>
                                   </div>

                                   <button type="submit" class="btn btn-primary">Add Equipment</button>
                              </form>
                         </div>
                    </div>

                    <div class="col-md-6 col-sm-6">
                         <div class="feature-thumb" style="margin-bottom:20px;">
                              <h3>Add Device Type</h3>
                              <form method="post" action="">
                                   <input type="hidden" name="action" value="add_device_type">
                                   <div class="form-group">
                                        <label for="device_type_name">Device Type Name:</label>
                                        <input type="text" class="form-control" id="device_type_name" name="device_type_name" required>
                                   </div>
                                   <button type="submit" class="btn btn-primary">Add Device Type</button>
                              </form>
                         </div>

                         <div class="feature-thumb">
                              <h3>Add Manufacturer</h3>
                              <form method="post" action="">
                                   <input type="hidden" name="action" value="add_manufacturer">
                                   <div class="form-group">
                                        <label for="manufacturer_name">Manufacturer Name:</label>
                                        <input type="text" class="form-control" id="manufacturer_name" name="manufacturer_name" required>
                                   </div>
                                   <button type="submit" class="btn btn-primary">Add Manufacturer</button>
                              </form>
                         </div>
                    </div>
               </div>
          </div>
     </section>
</body>
</html>
<?php $dblink->close(); ?>
