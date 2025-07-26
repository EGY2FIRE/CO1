<?php
require_once 'config.php';

// ستحتاج إلى مكتبة لإرسال البريد الإلكتروني مثل PHPMailer
// composer require phpmailer/phpmailer

// عدد الأيام قبل انتهاء الصلاحية لإرسال التنبيه
$days_before_expiry = 14;

$stmt = $pdo->prepare("SELECT fp.id, fp.page_name, fp.token_expiry_date, u.email, u.username
                       FROM facebook_pages fp
                       JOIN users u ON fp.user_id = u.id
                       WHERE fp.token_expiry_date IS NOT NULL
                         AND fp.token_expiry_date <= DATE_ADD(CURDATE(), INTERVAL :days_before_expiry DAY)
                         AND fp.is_active = TRUE"); // فقط للصفحات المفعلة
$stmt->execute([':days_before_expiry' => $days_before_expiry]);
$expiringTokens = $stmt->fetchAll();

foreach ($expiringTokens as $token_info) {
    $email = $token_info['email'];
    $username = $token_info['username'];
    $pageName = $token_info['page_name'];
    $expiryDate = new DateTime($token_info['token_expiry_date']);
    $today = new DateTime();
    $interval = $today->diff($expiryDate);
    $remainingDays = $interval->days;

    // أرسل إيميل تنبيه
    $subject = "تنبيه: صلاحية Access Token لصفحتك على الفيسبوك " . $pageName . " على وشك الانتهاء";
    $message = "مرحبًا " . $username . "،\n\n";
    $message .= "صلاحية Access Token لصفحة الفيسبوك الخاصة بك " . $pageName . " ستنتهي خلال " . $remainingDays . " يومًا. يرجى تجديده لتجنب توقف الردود التلقائية.\n\n";
    $message .= "يمكنك تجديده من خلال لوحة التحكم الخاصة بك على الموقع.\n\n";
    $message .= "شكرًا لك،\nفريق " . $_SERVER['HTTP_HOST']; // استبدل باسم موقعك

    // هنا ستحتاج لاستخدام PHPMailer أو أي مكتبة لإرسال البريد الإلكتروني
    // مثال (افتراضي):
    // send_email($email, $subject, $message);
    error_log("Sending expiry warning email to " . $email . " for page " . $pageName); // سجل في log كاختبار
}

unset($pdo);
?>