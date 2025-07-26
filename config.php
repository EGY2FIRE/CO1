<?php
ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);
// إعدادات قاعدة البيانات
define('DB_SERVER', 'localhost');
define('DB_USERNAME', 'u434274691_co'); // اسم مستخدم قاعدة البيانات الخاصة بك
define('DB_PASSWORD', 'Apaza@1994'); // كلمة مرور قاعدة البيانات الخاصة بك
define('DB_NAME', 'u434274691_co'); // اسم قاعدة البيانات الخاصة بك

// إعدادات Facebook App (يجب إنشاؤه من لوحة مطوري الفيسبوك)
define('FB_APP_ID', '2176381729367645');     // معرف تطبيق فيسبوك الخاص بك
define('FB_APP_SECRET', 'ac163a02029faef04c82e90228539f3c'); // سكرت تطبيق فيسبوك الخاص بك
define('FB_GRAPH_VERSION', 'v19.0'); // تأكد من استخدام أحدث إصدار من Graph API

// رابط إعادة التوجيه بعد مصادقة فيسبوك
define('FB_REDIRECT_URI', 'https://co.apaza.cloud/callback.php'); // مسار ملف الـ callback الفعلي وبـ https

// **** إعداد المنطقة الزمنية الافتراضية لـ PHP (مهم للتواريخ) ****
date_default_timezone_set('Africa/Cairo');

// الاتصال بقاعدة البيانات
try {
    $pdo = new PDO("mysql:host=" . DB_SERVER . ";dbname=" . DB_NAME, DB_USERNAME, DB_PASSWORD);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::FETCH_ASSOC);
    $pdo->setAttribute(PDO::ATTR_DEFAULT_FETCH_MODE, PDO::FETCH_ASSOC);
    $pdo->exec("set names utf8mb4");

    // ** ملاحظة هامة: تم إزالة الكود الخاص بإنشاء الجداول تلقائيًا من هنا. **
    // ** يجب التأكد من تحديث بنية قاعدة البيانات يدويًا باستخدام أوامر SQL التي تم توفيرها سابقًا. **
    // ** الكود الذي تم إزالته كان يحتوي على بنية جداول قديمة وغير متوافقة مع أحدث ميزات التطبيق. **

} catch (PDOException $e) {
    die("ERROR: Could not connect to database. " . $e->getMessage());
}

// بدء الجلسة (للتسجيل والدخول)
if (session_status() == PHP_SESSION_NONE) {
    session_start();
}
?>
