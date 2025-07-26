<?php
// cron_rotate_accounts.php
// هذا السكريبت مسؤول عن تبديل الحساب النشط لكل صفحة كل 10 دقائق.
// يجب تشغيله بواسطة Cron Job كل دقيقة (أو كل 5 دقائق كحد أقصى).

require_once __DIR__ . '/../config.php'; // تضمين ملف الإعدادات

// دالة تسجيل الأنشطة
function log_activity($message, $level = 'info') {
    $log_directory = __DIR__ . '/../logs/';
    if (!is_dir($log_directory)) {
        if (!mkdir($log_directory, 0755, true)) {
            error_log("Failed to create log directory: " . $log_directory);
            error_log("[$level] $message");
            return;
        }
    }
    $log_file = $log_directory . 'rotation_activity.log'; // سجل خاص بالـ Rotation
    $timestamp = date('Y-m-d H:i:s');
    file_put_contents($log_file, "[$timestamp][$level] $message\n", FILE_APPEND);
}

log_activity("Rotation cron job started. " . date('Y-m-d H:i:s'), "info");

try {
    // جلب جميع الصفحات التي لديها حسابات مفعلة
    $stmtPages = $pdo->prepare("SELECT fp.id AS page_db_id, fp.page_name, fp.current_rotation_account_id, fp.last_rotation_time
                                FROM facebook_pages fp
                                WHERE fp.user_id IN (SELECT DISTINCT user_id FROM facebook_page_accounts WHERE is_active = TRUE)"); // فقط الصفحات التي يملكها مستخدم لديه حسابات مفعلة
    $stmtPages->execute();
    $pages = $stmtPages->fetchAll();

    $nowUtc = new DateTime('now', new DateTimeZone('UTC'));

    foreach ($pages as $page) {
        $pageDbId = $page['page_db_id'];
        $pageName = $page['page_name'];
        $currentRotationAccountId = $page['current_rotation_account_id'];
        $lastRotationTime = $page['last_rotation_time'];

        $rotate = false;
        if (empty($currentRotationAccountId)) {
            $rotate = true; // لا يوجد حساب نشط حاليا، يجب اختيار الأول
            log_activity("Page '{$pageName}' has no current rotation account. Initializing rotation.", "debug");
        } else {
            $lastRotationDateTime = new DateTime($lastRotationTime, new DateTimeZone('UTC'));
            $intervalSeconds = $nowUtc->getTimestamp() - $lastRotationDateTime->getTimestamp();

            if ($intervalSeconds >= (ROTATION_INTERVAL_MINUTES * 60)) {
                $rotate = true; // حان وقت التبديل
                log_activity("Rotation interval (10 mins) passed for page '{$pageName}'. Rotating account.", "debug");
            } else {
                // تحقق إذا كان الحساب الحالي لا يزال نشطًا. إذا لم يكن، يجب التبديل.
                $stmtCheckCurrent = $pdo->prepare("SELECT id FROM facebook_page_accounts WHERE id = :id AND is_active = TRUE");
                $stmtCheckCurrent->execute([':id' => $currentRotationAccountId]);
                if ($stmtCheckCurrent->rowCount() == 0) {
                    $rotate = true;
                    log_activity("Current rotation account {$currentRotationAccountId} for page '{$pageName}' is inactive. Forcing rotation.", "debug");
                }
            }
        }

        if ($rotate) {
            // جلب جميع الحسابات النشطة للصفحة، مرتبة حسب ID (لضمان ترتيب ثابت)
            $stmtActiveAccounts = $pdo->prepare("SELECT id FROM facebook_page_accounts WHERE page_id = :page_id AND is_active = TRUE ORDER BY id ASC");
            $stmtActiveAccounts->execute([':page_id' => $pageDbId]);
            $activeAccounts = $stmtActiveAccounts->fetchAll(PDO::FETCH_COLUMN);

            if (empty($activeAccounts)) {
                // لا توجد حسابات نشطة لهذه الصفحة، قم بإزالة الحساب الحالي من الدوران
                if (!empty($currentRotationAccountId)) {
                    $stmtUpdatePage = $pdo->prepare("UPDATE facebook_pages SET current_rotation_account_id = NULL, last_rotation_time = UTC_TIMESTAMP() WHERE id = :page_id");
                    $stmtUpdatePage->execute([':page_id' => $pageDbId]);
                    log_activity("No active accounts found for page '{$pageName}'. Set current_rotation_account_id to NULL.", "warning");
                }
                continue; // تخطي هذه الصفحة
            }

            $nextAccountIndex = 0;
            if (!empty($currentRotationAccountId) && in_array($currentRotationAccountId, $activeAccounts)) {
                $currentIndex = array_search($currentRotationAccountId, $activeAccounts);
                $nextAccountIndex = ($currentIndex + 1) % count($activeAccounts);
            }
            $nextRotationAccountId = $activeAccounts[$nextAccountIndex];

            // تحديث الصفحة بالحساب الجديد ووقت التبديل
            $stmtUpdatePage = $pdo->prepare("UPDATE facebook_pages SET current_rotation_account_id = :account_id, last_rotation_time = UTC_TIMESTAMP() WHERE id = :page_id");
            $stmtUpdatePage->execute([
                ':account_id' => $nextRotationAccountId,
                ':page_id' => $pageDbId
            ]);
            log_activity("Rotated page '{$pageName}' to account ID {$nextRotationAccountId}.", "info");
        }
    }
} catch (PDOException $e) {
    log_activity("Database error in rotation cron job: " . $e->getMessage(), "error");
} catch (Exception $e) {
    log_activity("General error in rotation cron job: " . $e->getMessage(), "error");
} finally {
    unset($pdo);
    log_activity("Rotation cron job finished successfully. " . date('Y-m-d H:i:s'), "info");
}
?>
