<?php
require_once 'auth_check.php'; // تأكد من تسجيل دخول المستخدم
require_once 'config.php';

// تضمين Facebook SDK (إذا كنت تستخدم Composer)
require_once 'path/to/facebook-php-sdk-v5/src/Facebook/autoload.php'; // استبدل بالمسار الصحيح

$fb = new Facebook\Facebook([
    'app_id' => FB_APP_ID,
    'app_secret' => FB_APP_SECRET,
    'default_graph_version' => FB_GRAPH_VERSION,
]);

$helper = $fb->getRedirectLoginHelper();

$permissions = [
    'public_profile',
    'pages_show_list',
    'pages_read_engagement',
    'pages_manage_posts',
    'pages_manage_engagement'
]; // الأذونات المطلوبة

$loginUrl = $helper->getLoginUrl(FB_REDIRECT_URI, $permissions);
?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>إضافة صفحة فيسبوك</title>
    <link rel="stylesheet" href="style.css"> <!-- ستحتاج لتصميم ملف style.css -->
</head>
<body>
    <?php include 'navbar.php'; // قائمة التنقل ?>
    <div class="wrapper">
        <h2>إضافة صفحة فيسبوك جديدة</h2>
        <p>لإضافة صفحة فيسبوك، يرجى النقر على الزر أدناه ومنح الأذونات اللازمة لتطبيقنا.</p>
        <p>سيتم تطبيق الإعدادات الافتراضية الخاصة بك على الحساب الجديد.</p>
        <a href="<?php echo htmlspecialchars($loginUrl); ?>" class="btn btn-primary">ربط صفحة فيسبوك</a>
    </div>
</body>
</html>
