<?php
require_once 'config.php';

// ستحتاج إلى مكتبة لإرسال البريد الإلكتروني مثل PHPMailer
// composer require phpmailer/phpmailer

$email_err = $success_message = '';

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    $email = trim($_POST['email']);

    if (empty($email)) {
        $email_err = 'الرجاء إدخال بريدك الإلكتروني.';
    } elseif (!filter_var($email, FILTER_VALIDATE_EMAIL)) {
        $email_err = 'صيغة البريد الإلكتروني غير صحيحة.';
    } else {
        $sql = "SELECT id FROM users WHERE email = :email";
        if ($stmt = $pdo->prepare($sql)) {
            $stmt->bindParam(':email', $param_email, PDO::PARAM_STR);
            $param_email = $email;
            if ($stmt->execute()) {
                if ($stmt->rowCount() == 1) {
                    // المستخدم موجود، قم بإنشاء توكين لإعادة تعيين كلمة المرور
                    // وفي سيناريو حقيقي: قم بحفظ التوكين وتاريخ صلاحيته في جدول جديد
                    // وأرسل رابط يحتوي على هذا التوكين للبريد الإلكتروني.
                    // مثال: http://yourdomain.com/reset_password.php?token=some_unique_token

                    // هذا الجزء يحتاج إلى مكتبة PHPMailer وإعداد SMTP
                    // require 'vendor/autoload.php'; // إذا كنت تستخدم Composer
                    // use PHPMailer\PHPMailer\PHPMailer;
                    // use PHPMailer\PHPMailer\Exception;

                    // $mail = new PHPMailer(true);
                    // ... إعدادات SMTP و إرسال الإيميل ...

                    $success_message = "تم إرسال رابط إعادة تعيين كلمة المرور إلى بريدك الإلكتروني.";
                } else {
                    $email_err = 'لا يوجد حساب بهذا البريد الإلكتروني.';
                }
            } else {
                echo "حدث خطأ ما. يرجى المحاولة لاحقًا.";
            }
        }
        unset($stmt);
    }
}
?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>نسيت كلمة المرور</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <div class="wrapper">
        <h2>نسيت كلمة المرور؟</h2>
        <p>الرجاء إدخال بريدك الإلكتروني لإعادة تعيين كلمة المرور.</p>
        <?php if (!empty($success_message)): ?>
            <p class="success-message"><?php echo $success_message; ?></p>
        <?php endif; ?>
        <form action="<?php echo htmlspecialchars($_SERVER['PHP_SELF']); ?>" method="post">
            <div class="form-group">
                <label>البريد الإلكتروني</label>
                <input type="email" name="email" class="form-control <?php echo (!empty($email_err)) ? 'is-invalid' : ''; ?>" value="<?php echo htmlspecialchars($_POST['email'] ?? ''); ?>">
                <span class="invalid-feedback"><?php echo $email_err; ?></span>
            </div>
            <div class="form-group">
                <input type="submit" class="btn btn-primary" value="إعادة تعيين كلمة المرور">
            </div>
            <p>تذكرت كلمة المرور؟ <a href="login.php">سجل دخولك</a>.</p>
        </form>
    </div>
</body>
</html>
<?php unset($pdo); ?>