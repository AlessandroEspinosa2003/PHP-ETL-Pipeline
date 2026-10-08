<?php
include("functions.php");

$dblink = db_connect("equipment_prod");
$id = (int)($_GET['id'] ?? $_POST['id'] ?? 0);
$msg = $_GET['msg'] ?? '';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $action = $_POST['action'] ?? '';

    if ($action === 'modify_equipment') {
        $deviceTypeId = (int)($_POST['device_type_id'] ?? 0);
        $manufacturerId = (int)($_POST['manufacturer_id'] ?? 0);
        $serialNumber = strtoupper(trim($_POST['serial_number'] ?? ''));
        $status = ($_POST['status'] ?? 'active') === 'inactive' ? 'inactive' : 'active';

        if ($deviceTypeId <= 0 || $manufacturerId <= 0) {
            redirect("modify.php?id=" . $id . "&msg=InvalidSelection");
        }

        if (!valid_serial($serialNumber)) {
            redirect("modify.php?id=" . $id . "&msg=InvalidSerial");
        }

        $stmt = $dblink->prepare("SELECT id FROM equipment WHERE serial_number=? AND id<>?");
        $stmt->bind_param("si", $serialNumber, $id);
        $stmt->execute();
        $exists = $stmt->get_result()->num_rows > 0;
        $stmt->close();

        if ($exists) {
            redirect("modify.php?id=" . $id . "&msg=SerialExists");
        }

        $stmt = $dblink->prepare(
            "UPDATE equipment
             SET device_type_id=?, manufacturer_id=?, serial_number=?, status=?
             WHERE id=?"
        );
        $stmt->bind_param("iissi", $deviceTypeId, $manufacturerId, $serialNumber, $status, $id);
        $stmt->execute();
        $stmt->close();

        redirect("index.php?msg=EquipmentModified");
    }

    if ($action === 'modify_device_type') {
        $deviceTypeId = (int)($_POST['device_type_lookup_id'] ?? 0);
        $name = trim($_POST['device_type_name'] ?? '');
        $status = ($_POST['device_type_status'] ?? 'active') === 'inactive' ? 'inactive' : 'active';

        if ($deviceTypeId <= 0 || $name === '' || !valid_alpha_spaces($name)) {
            redirect("modify.php?id=" . $id . "&msg=InvalidDeviceType");
        }

        $stmt = $dblink->prepare("SELECT id FROM device_types WHERE LOWER(name)=LOWER(?) AND id<>?");
        $stmt->bind_param("si", $name, $deviceTypeId);
        $stmt->execute();
        $exists = $stmt->get_result()->num_rows > 0;
        $stmt->close();

        if ($exists) {
            redirect("modify.php?id=" . $id . "&msg=DeviceTypeExists");
        }

        $stmt = $dblink->prepare("UPDATE device_types SET name=?, status=? WHERE id=?");
        $stmt->bind_param("ssi", $name, $status, $deviceTypeId);
        $stmt->execute();
        $stmt->close();

        redirect("index.php?msg=DeviceTypeModified");
    }

    if ($action === 'modify_manufacturer') {
        $manufacturerId = (int)($_POST['manufacturer_lookup_id'] ?? 0);
        $name = trim($_POST['manufacturer_name'] ?? '');
        $status = ($_POST['manufacturer_status'] ?? 'active') === 'inactive' ? 'inactive' : 'active';

        if ($manufacturerId <= 0 || $name === '' || !valid_alpha_spaces($name)) {
            redirect("modify.php?id=" . $id . "&msg=InvalidManufacturer");
        }

        $stmt = $dblink->prepare("SELECT id FROM manufacturers WHERE LOWER(name)=LOWER(?) AND id<>?");
        $stmt->bind_param("si", $name, $manufacturerId);
        $stmt->execute();
        $exists = $stmt->get_result()->num_rows > 0;
        $stmt->close();

        if ($exists) {
            redirect("modify.php?id=" . $id . "&msg=ManufacturerExists");
        }

        $stmt = $dblink->prepare("UPDATE manufacturers SET name=?, status=? WHERE id=?");
        $stmt->bind_param("ssi", $name, $status, $manufacturerId);
        $stmt->execute();
        $stmt->close();

        redirect("index.php?msg=ManufacturerModified");
    }
}

$deviceTypes = fetch_active_device_types($dblink);
$manufacturers = fetch_active_manufacturers($dblink);

$stmt = $dblink->prepare(
    "SELECT
        e.id,
        e.device_type_id,
        e.manufacturer_id,
        e.serial_number,
        e.status,
        dt.name AS device_type_name,
        m.name AS manufacturer_name
     FROM equipment e
     JOIN device_types dt ON e.device_type_id = dt.id
     JOIN manufacturers m ON e.manufacturer_id = m.id
     WHERE e.id=?"
);
$stmt->bind_param("i", $id);
$stmt->execute();
$equipment = $stmt->get_result()->fetch_assoc();
$stmt->close();

if (!$equipment) {
    die("Equipment not found.");
}

$stmt = $dblink->prepare("SELECT id, name, status FROM device_types WHERE id=?");
$stmt->bind_param("i", $equipment['device_type_id']);
$stmt->execute();
$currentDeviceType = $stmt->get_result()->fetch_assoc();
$stmt->close();

$stmt = $dblink->prepare("SELECT id, name, status FROM manufacturers WHERE id=?");
$stmt->bind_param("i", $equipment['manufacturer_id']);
$stmt->execute();
$currentManufacturer = $stmt->get_result()->fetch_assoc();
$stmt->close();
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
                    <a href="#" class="navbar-brand">Modify Equipment</a>
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
                         <?php if ($msg === "InvalidSelection"): ?>
                              <div class="alert alert-danger" role="alert">Please select valid active device type and manufacturer values.</div>
                         <?php elseif ($msg === "InvalidSerial"): ?>
                              <div class="alert alert-danger" role="alert">Serial number must start with SN- and contain only alphanumeric characters after it.</div>
                         <?php elseif ($msg === "SerialExists"): ?>
                              <div class="alert alert-danger" role="alert">That serial number already belongs to another equipment record.</div>
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

                    <div class="col-md-12">
                         <div class="feature-thumb" style="margin-bottom:20px;">
                              <h3>Modify Equipment</h3>
                              <form method="post" action="">
                                   <input type="hidden" name="action" value="modify_equipment">
                                   <input type="hidden" name="id" value="<?php echo (int)$equipment['id']; ?>">

                                   <div class="form-group">
                                        <label for="device_type_id">Device Type:</label>
                                        <select class="form-control" name="device_type_id" id="device_type_id" required>
                                             <?php foreach ($deviceTypes as $device): ?>
                                                  <option value="<?php echo (int)$device['id']; ?>" <?php if ((int)$equipment['device_type_id'] === (int)$device['id']) echo 'selected'; ?>>
                                                       <?php echo h($device['name']); ?>
                                                  </option>
                                             <?php endforeach; ?>
                                        </select>
                                   </div>

                                   <div class="form-group">
                                        <label for="manufacturer_id">Manufacturer:</label>
                                        <select class="form-control" name="manufacturer_id" id="manufacturer_id" required>
                                             <?php foreach ($manufacturers as $manufacturer): ?>
                                                  <option value="<?php echo (int)$manufacturer['id']; ?>" <?php if ((int)$equipment['manufacturer_id'] === (int)$manufacturer['id']) echo 'selected'; ?>>
                                                       <?php echo h($manufacturer['name']); ?>
                                                  </option>
                                             <?php endforeach; ?>
                                        </select>
                                   </div>

                                   <div class="form-group">
                                        <label for="serial_number">Serial Number:</label>
                                        <input type="text" class="form-control" id="serial_number" name="serial_number" value="<?php echo h($equipment['serial_number']); ?>" required>
                                   </div>

                                   <div class="form-group">
                                        <label for="status">Equipment Status:</label>
                                        <select class="form-control" name="status" id="status">
                                             <option value="active" <?php if ($equipment['status'] === 'active') echo 'selected'; ?>>active</option>
                                             <option value="inactive" <?php if ($equipment['status'] === 'inactive') echo 'selected'; ?>>inactive</option>
                                        </select>
                                   </div>

                                   <button type="submit" class="btn btn-primary">Update Equipment</button>
                              </form>
                         </div>
                    </div>

                    <div class="col-md-6 col-sm-6">
                         <div class="feature-thumb">
                              <h3>Modify Current Device Type</h3>
                              <form method="post" action="">
                                   <input type="hidden" name="action" value="modify_device_type">
                                   <input type="hidden" name="id" value="<?php echo (int)$equipment['id']; ?>">
                                   <input type="hidden" name="device_type_lookup_id" value="<?php echo (int)$currentDeviceType['id']; ?>">

                                   <div class="form-group">
                                        <label for="device_type_name">Device Type Name:</label>
                                        <input type="text" class="form-control" id="device_type_name" name="device_type_name" value="<?php echo h($currentDeviceType['name']); ?>" required>
                                   </div>

                                   <div class="form-group">
                                        <label for="device_type_status">Device Type Status:</label>
                                        <select class="form-control" name="device_type_status" id="device_type_status">
                                             <option value="active" <?php if ($currentDeviceType['status'] === 'active') echo 'selected'; ?>>active</option>
                                             <option value="inactive" <?php if ($currentDeviceType['status'] === 'inactive') echo 'selected'; ?>>inactive</option>
                                        </select>
                                   </div>

                                   <button type="submit" class="btn btn-primary">Update Device Type</button>
                              </form>
                         </div>
                    </div>

                    <div class="col-md-6 col-sm-6">
                         <div class="feature-thumb">
                              <h3>Modify Current Manufacturer</h3>
                              <form method="post" action="">
                                   <input type="hidden" name="action" value="modify_manufacturer">
                                   <input type="hidden" name="id" value="<?php echo (int)$equipment['id']; ?>">
                                   <input type="hidden" name="manufacturer_lookup_id" value="<?php echo (int)$currentManufacturer['id']; ?>">

                                   <div class="form-group">
                                        <label for="manufacturer_name">Manufacturer Name:</label>
                                        <input type="text" class="form-control" id="manufacturer_name" name="manufacturer_name" value="<?php echo h($currentManufacturer['name']); ?>" required>
                                   </div>

                                   <div class="form-group">
                                        <label for="manufacturer_status">Manufacturer Status:</label>
                                        <select class="form-control" name="manufacturer_status" id="manufacturer_status">
                                             <option value="active" <?php if ($currentManufacturer['status'] === 'active') echo 'selected'; ?>>active</option>
                                             <option value="inactive" <?php if ($currentManufacturer['status'] === 'inactive') echo 'selected'; ?>>inactive</option>
                                        </select>
                                   </div>

                                   <button type="submit" class="btn btn-primary">Update Manufacturer</button>
                              </form>
                         </div>
                    </div>
               </div>
          </div>
     </section>
</body>
</html>
<?php $dblink->close(); ?>

