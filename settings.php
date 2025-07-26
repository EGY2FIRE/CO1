<?php
require_once 'auth_check.php';
require_once 'config.php';

$current_user_id = $_SESSION['user_id'];
$success_message = '';
$error_message = '';

// جلب الإعدادات الافتراضية الحالية للمستخدم
$stmt = $pdo->prepare("SELECT default_reply_interval_seconds, default_comments_limit_per_run, default_custom_since_hours FROM users WHERE id = :user_id");
$stmt->execute([':user_id' => $current_user_id]);
$user_settings = $stmt->fetch();

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    $default_interval = intval($_POST['default_reply_interval_seconds']);
    $default_limit = intval($_POST['default_comments_limit_per_run']);
    $default_since = intval($_POST['default_custom_since_hours']);
    // **** إضافة الإعدادات الافتراضية الجديدة ****
    $default_rotation_duration = intval($_POST['default_rotation_duration_minutes']);
    $default_max_replies_per_hour = intval($_POST['default_max_replies_per_hour']);
    // ****************************************

    // التحقق من القيم المدخلة
    if ($default_interval < 1 || $default_limit < 1 || $default_since < 0 || $default_rotation_duration < 1 || $default_max_replies_per_hour < 0) {
        $error_message = 'الرجاء إدخال قيم صحيحة (الفاصل الزمني، الحد الأقصى للتعليقات في التشغيل، ومدة الدورة يجب أن تكون 1 أو أكثر. مدة البحث والحد الأقصى للردود في الساعة 0 أو أكثر).';
    } else {
        $stmtUpdate = $pdo->prepare("UPDATE users SET 
            default_reply_interval_seconds = :interval, 
            default_comments_limit_per_run = :limit, 
            default_custom_since_hours = :since,
            rotation_duration_minutes = :rotation_duration,
            max_replies_per_hour = :max_replies_per_hour
            WHERE id = :user_id");
        $stmtUpdate->execute([
            ':interval' => $default_interval,
            ':limit' => $default_limit,
            ':since' => $default_since,
            ':rotation_duration' => $default_rotation_duration,
            ':max_replies_per_hour' => $default_max_replies_per_hour,
            ':user_id' => $current_user_id
        ]);
        $success_message = 'تم تحديث الإعدادات الافتراضية بنجاح.';
        // إعادة جلب الإعدادات المحدثة للعرض
        $stmt = $pdo->prepare("SELECT default_reply_interval_seconds, default_comments_limit_per_run, default_custom_since_hours, rotation_duration_minutes, max_replies_per_hour FROM users WHERE id = :user_id");
        $stmt->execute([':user_id' => $current_user_id]);
        $user_settings = $stmt->fetch();
    }
}
?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>الإعدادات الافتراضية</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <?php include 'navbar.php'; ?>
    <div class="wrapper">
        <h2>الإعدادات الافتراضية</h2>
        <p>هذه الإعدادات ستُطبق تلقائيًا على أي حسابات فيسبوك جديدة تقوم بربطها.</p>

        <?php if (!empty($success_message)): ?>
            <p style="color: green; font-weight: bold;"><?php echo htmlspecialchars($success_message); ?></p>
        <?php endif; ?>
        <?php if (!empty($error_message)): ?>
            <p style="color: red; font-weight: bold;"><?php echo htmlspecialchars($error_message); ?></p>
        <?php endif; ?>

        <form action="" method="post">
            <div class="form-group">
                <label>الفاصل الزمني الافتراضي بين الردود (ثوانٍ):</label>
                <input type="number" name="default_reply_interval_seconds" class="form-control" value="<?php echo htmlspecialchars($user_settings['default_reply_interval_seconds']); ?>" min="1" required>
            </div>
            <div class="form-group">
                <label>عدد التعليقات الافتراضي في كل تشغيل:</label>
                <input type="number" name="default_comments_limit_per_run" class="form-control" value="<?php echo htmlspecialchars($user_settings['default_comments_limit_per_run']); ?>" min="1" required>
            </div>
            <div class="form-group">
                <label>مدة البحث الافتراضية عن التعليقات (بالساعات، 0 لجميع التعليقات):</label>
                <input type="number" name="default_custom_since_hours" class="form-control" value="<?php echo htmlspecialchars($user_settings['default_custom_since_hours']); ?>" min="0" required>
            </div>
            <div class="form-group">
                <label>مدة دورة الحساب الافتراضية (بالدقائق):</label>
                <input type="number" name="default_rotation_duration_minutes" class="form-control" value="<?php echo htmlspecialchars($user_settings['rotation_duration_minutes']); ?>" min="1" required>
            </div>
            <div class="form-group">
                <label>الحد الأقصى الافتراضي للردود في الساعة:</label>
                <input type="number" name="default_max_replies_per_hour" class="form-control" value="<?php echo htmlspecialchars($user_settings['max_replies_per_hour']); ?>" min="0" required>
            </div>
            <div class="form-group">
                <button type="submit" class="btn btn-primary">حفظ الإعدادات الافتراضية</button>
            </div>
        </form>
    </div>
</body>
</html>
