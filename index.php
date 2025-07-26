<?php
require_once 'auth_check.php'; // تأكد من تسجيل دخول المستخدم
require_once 'config.php';
?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>لوحة التحكم</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <?php include 'navbar.php'; ?>
    <div class="wrapper">
        <h2>مرحباً بك يا <?php echo htmlspecialchars($_SESSION['username']); ?>!</h2>
        <p>هذه هي لوحة التحكم الخاصة بك.</p>
        <ul>
            <li><a href="pages.php">إدارة صفحات الفيسبوك</a></li>
            <li><a href="add_page.php">إضافة صفحة فيسبوك جديدة</a></li>
            </ul>
    </div>
</body>
</html>