<?php
require_once 'config.php';

// ستحتاج إلى مكتبة لإرسال البريد الإلكتروني مثل PHPMailer
// composer require phpmailer/phpmailer

$days_before_expiry = 14; // عدد الأيام قبل انتهاء الصلاحية لإرسال التنبيه

$stmt = $pdo->prepare("SELECT fpa.id, fpa.account_name, fp.page_name, fpa.token_expiry_date, u.email, u.username
                       FROM facebook_page_accounts fpa
                       JOIN facebook_pages fp ON fpa.page_id = fp.id
                       JOIN users u ON fpa.user_id = u.id
                       WHERE fpa.token_expiry_date IS NOT NULL
                         AND fpa.token_expiry_date <= DATE_ADD(CURDATE(), INTERVAL :days_before_expiry DAY)
                         AND fpa.is_active = TRUE");
$stmt->execute([':days_before_expiry' => $days_before_expiry]);
$expiringTokens = $stmt->fetchAll();

foreach ($expiringTokens as $token_info) {
    $email = $token_info['email'];
    $username = $token_info['username'];
    $pageName = $token_info['page_name'];
    $accountName = $token_info['account_name'];
    $expiryDate = new DateTime($token_info['token_expiry_date']);
    $today = new DateTime();
    $interval = $today->diff($expiryDate);
    $remainingDays = $interval->days;

    $subject = "تنبيه: صلاحية Access Token لحساب " . $accountName . " على صفحة " . $pageName . " على وشك الانتهاء";
    $message = "مرحبًا " . $username . "،\n\n";
    $message .= "صلاحية Access Token لحساب الفيسبوك الخاص بك (".$accountName.") على صفحة " . $pageName . " ستنتهي خلال " . $remainingDays . " يومًا. يرجى تجديده لتجنب توقف الردود التلقائية.\n\n";
    $message .= "يمكنك تجديده من خلال لوحة التحكم الخاصة بك على الموقع بالذهاب إلى صفحة إدارة الصفحات ثم النقر على زر 'ربط صفحة جديدة' لإعادة مصادقة الحساب.\n\n";
    $message .= "شكرًا لك،\nفريق " . $_SERVER['HTTP_HOST'];

    // هنا ستحتاج لاستخدام PHPMailer أو أي مكتبة لإرسال البريد الإلكتروني
    // مثال (افتراضي):
    // send_email($email, $subject, $message);
    error_log("Sending expiry warning email to " . $email . " for account " . $accountName . " on page " . $pageName);
}

unset($pdo);
?>