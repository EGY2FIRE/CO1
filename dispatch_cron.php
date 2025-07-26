<?php
// dispatch_cron.php
// هذا السكريبت سيتم تشغيله بواسطة Cron Job الرئيسي (مثلاً كل 5 دقائق).
// وظيفته هي جلب جميع الحسابات المفعلة وتشغيل cron_auto_reply.php لكل حساب كعملية منفصلة.

// 1. تضمين ملف الإعدادات والاتصال بقاعدة البيانات
require_once __DIR__ . '/config.php';

// 2. دالة بسيطة لتسجيل الأنشطة والأخطاء (نفس الدالة في cron_auto_reply.php)
function log_activity($message, $level = 'info') {
    $log_directory = __DIR__ . '/logs/'; // مجلد logs في نفس مستوى config.php
    if (!is_dir($log_directory)) {
        if (!mkdir($log_directory, 0755, true)) {
            error_log("Failed to create log directory: " . $log_directory);
            error_log("[$level] $message");
            return;
        }
    }
    $log_file = $log_directory . 'bot_activity.log';
    $timestamp = date('Y-m-d H:i:s');
    file_put_contents($log_file, "[$timestamp][$level] $message\n", FILE_APPEND);
}

log_activity("Dispatcher cron job started. " . date('Y-m-d H:i:s'), "info");

// 3. جلب جميع معرفات الحسابات المفعلة
try {
    $stmt = $pdo->prepare("SELECT id FROM facebook_page_accounts WHERE is_active = TRUE");
    $stmt->execute();
    $activeAccountIds = $stmt->fetchAll(PDO::FETCH_COLUMN); // جلب فقط عمود الـ ID

    log_activity("Found " . count($activeAccountIds) . " active accounts to dispatch.", "info");

} catch (PDOException $e) {
    log_activity("Database error when fetching active account IDs for dispatch: " . $e->getMessage(), "error");
    unset($pdo);
    exit();
}

unset($pdo); // إغلاق اتصال قاعدة البيانات في الموزع مبكراً

// 4. تشغيل cron_auto_reply.php لكل حساب كعملية خلفية منفصلة
$cron_script_path = __DIR__ . '/cron_jobs/cron_auto_reply.php'; // المسار إلى سكريبت المعالجة الفعلي
$php_cli_path = '/usr/bin/php'; // المسار لمفسر PHP CLI على خادمك (تحقق منه)

if (!file_exists($cron_script_path)) {
    log_activity("Error: cron_auto_reply.php not found at " . $cron_script_path, "critical");
    exit();
}

foreach ($activeAccountIds as $accountId) {
    // بناء الأمر لتشغيل السكريبت كعملية خلفية
    // > /dev/null 2>&1 : لإعادة توجيه الإخراج والأخطاء إلى لا شيء (تجنب رسائل البريد الإلكتروني من الكرون)
    // & : لتشغيل العملية في الخلفية
    $command = "{$php_cli_path} {$cron_script_path} {$accountId} > /dev/null 2>&1 &";
    
    // تنفيذ الأمر
    exec($command, $output, $return_var);

    if ($return_var !== 0) {
        log_activity("Failed to dispatch cron for account ID {$accountId}. Command: '{$command}'. Return var: {$return_var}. Output: " . implode("\n", $output), "error");
    } else {
        log_activity("Successfully dispatched cron for account ID {$accountId}.", "info");
    }
}

log_activity("Dispatcher cron job finished. " . date('Y-m-d H:i:s'), "info");
?>
