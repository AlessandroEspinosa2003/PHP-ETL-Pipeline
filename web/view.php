<?php
include("functions.php");

$dblink = db_connect("equipment_prod");
$id = (int)($_GET['id'] ?? 0);

$stmt = $dblink->prepare(
    "SELECT
        e.id,
        dt.name AS device_type,
        m.name AS manufacturer,
        e.serial_number,
        e.status
     FROM equipment e
     JOIN device_types dt ON e.device_type_id = dt.id
     JOIN manufacturers m ON e.manufacturer_id = m.id
     WHERE e.id=?"
);
$stmt->bind_param("i", $id);
$stmt->execute();
$row = $stmt->get_result()->fetch_assoc();
$stmt->close();

if (!$row) {
    die("Equipment not found.");
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
                    <a href="#" class="navbar-brand">View Equipment</a>
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
			      <h3>Equipment Details</h3>
                              <p><strong>Device ID:</strong> <?php echo h((string)$row['id']); ?></p>
                              <p><strong>Device Type:</strong> <?php echo h($row['device_type']); ?></p>
                              <p><strong>Manufacturer:</strong> <?php echo h($row['manufacturer']); ?></p>
                              <p><strong>Serial Number:</strong> <?php echo h($row['serial_number']); ?></p>
                              <p><strong>Status:</strong> <?php echo h($row['status']); ?></p>

                              <a href="modify.php?id=<?php echo urlencode((string)$row['id']); ?>" class="btn btn-primary">Modify</a>
                              <a href="search.php" class="btn btn-default">Back to Search</a>
                         </div>
                    </div>
               </div>
          </div>
     </section>
</body>
</html>
<?php $dblink->close(); ?>
