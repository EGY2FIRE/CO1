<?php
// global_templates.php
// هذا الملف مسؤول عن إدارة أسماء القوالب العامة للمستخدم الحالي.

// 1. تضمين ملفات التحقق من المصادقة والإعدادات
require_once 'auth_check.php'; // يتحقق من تسجيل دخول المستخدم
require_once 'config.php';     // يحتوي على إعدادات قاعدة البيانات والفيسبوك

// الحصول على معرف المستخدم الحالي من الجلسة
$current_user_id = $_SESSION['user_id'];

// 2. معالجة طلبات إضافة/تعديل/حذف أسماء القوالب العامة (POST requests)
$error_message = ''; // تهيئة رسالة الخطأ
$success_message = ''; // تهيئة رسالة النجاح

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    // إضافة قالب عام جديد
    if (isset($_POST['action']) && $_POST['action'] == 'add_global_template') {
        $template_name = trim($_POST['template_name']);
        
        // التحقق من أن الحقل ليس فارغًا
        if (!empty($template_name)) {
            try {
                $stmt = $pdo->prepare("INSERT INTO global_reply_templates (user_id, template_name) VALUES (:user_id, :template_name)");
                $stmt->execute([
                    ':user_id' => $current_user_id,
                    ':template_name' => $template_name
                ]);
                $success_message = "تمت إضافة مجموعة القوالب بنجاح.";
                // لا نحتاج لإعادة التوجيه هنا لأننا سنعرض الرسالة
                // header("location: global_templates.php"); exit();
            } catch (PDOException $e) {
                // التعامل مع خطأ فريد (مثل تكرار اسم القالب)
                if ($e->getCode() == 23000) { // SQLSTATE for integrity constraint violation
                    $error_message = "اسم القالب هذا موجود بالفعل. يرجى اختيار اسم آخر.";
                } else {
                    $error_message = "حدث خطأ أثناء إضافة القالب: " . $e->getMessage();
                }
            }
        } else {
            $error_message = "الرجاء إدخال اسم القالب.";
        }
    }

    // تعديل قالب عام موجود (اسم القالب أو حالة التفعيل أو الافتراضي)
    if (isset($_POST['action']) && $_POST['action'] == 'edit_global_template') {
        $template_id = $_POST['template_id'];
        $template_name = trim($_POST['template_name']);
        $is_active = isset($_POST['is_active']) ? 1 : 0; // تحويل قيمة checkbox إلى 0 أو 1
        $is_default = isset($_POST['is_default']) ? 1 : 0; // تحويل قيمة checkbox إلى 0 أو 1
        
        // التحقق من أن الحقل ليس فارغًا
        if (!empty($template_name)) {
            try {
                // إذا تم تعيين هذا القالب كافتراضي، نلغي تعيين أي قالب آخر كافتراضي للمستخدم
                if ($is_default) {
                    $stmt_reset_default = $pdo->prepare("UPDATE global_reply_templates SET is_default = FALSE WHERE user_id = :user_id AND id != :template_id");
                    $stmt_reset_default->execute([
                        ':user_id' => $current_user_id,
                        ':template_id' => $template_id
                    ]);
                }

                // تحديث القالب في قاعدة البيانات، مع التأكد من أنه يتبع المستخدم الصحيح
                $stmt = $pdo->prepare("UPDATE global_reply_templates SET template_name = :template_name, is_active = :is_active, is_default = :is_default WHERE id = :template_id AND user_id = :user_id");
                $stmt->execute([
                    ':template_name' => $template_name,
                    ':is_active' => $is_active,
                    ':is_default' => $is_default,
                    ':template_id' => $template_id,
                    ':user_id' => $current_user_id
                ]);
                $success_message = "تم تحديث مجموعة القوالب بنجاح.";
                // لا نحتاج لإعادة التوجيه هنا
                // header("location: global_templates.php"); exit();
            } catch (PDOException $e) {
                if ($e->getCode() == 23000) {
                    $error_message = "اسم القالب هذا موجود بالفعل. يرجى اختيار اسم آخر.";
                } else {
                    $error_message = "حدث خطأ أثناء تحديث القالب: " . $e->getMessage();
                }
            }
        } else {
            $error_message = "الرجاء إدخال اسم القالب.";
        }
    }

    // حذف قالب عام
    if (isset($_POST['action']) && $_POST['action'] == 'delete_global_template') {
        $template_id = $_POST['template_id'];
        
        // حذف القالب من قاعدة البيانات، مع التأكد من أنه يتبع المستخدم الصحيح
        // سيتم حذف جميع الردود المرتبطة به تلقائيًا بسبب ON DELETE CASCADE
        $stmt = $pdo->prepare("DELETE FROM global_reply_templates WHERE id = :template_id AND user_id = :user_id");
        $stmt->execute([':template_id' => $template_id, ':user_id' => $current_user_id]);
        $success_message = "تم حذف مجموعة القوالب بنجاح.";
        // لا نحتاج لإعادة التوجيه هنا
        // header("location: global_templates.php"); exit();
    }
}

// 3. استرجاع أسماء القوالب العامة للمستخدم الحالي لعرضها في الواجهة
$stmt = $pdo->prepare("SELECT * FROM global_reply_templates WHERE user_id = :user_id ORDER BY id DESC");
$stmt->execute([':user_id' => $current_user_id]);
$templates = $stmt->fetchAll();
?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>إدارة القوالب العامة</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <?php include 'navbar.php'; // تضمين شريط التنقل العلوي ?>
    <div class="wrapper">
        <h2>إدارة القوالب العامة</h2>
        <p>هنا يمكنك تعريف مجموعات من الردود العامة. سيتم استخدام هذه المجموعات للرد على الصفحات التي ليس لها قوالب ردود مخصصة. يمكنك تعيين مجموعة واحدة كـ **افتراضية**.</p>
        
        <?php if (isset($error_message) && !empty($error_message)): ?>
            <p style="color: red; font-weight: bold;"><?php echo htmlspecialchars($error_message); ?></p>
        <?php endif; ?>
        <?php if (isset($success_message) && !empty($success_message)): ?>
            <p style="color: green; font-weight: bold;"><?php echo htmlspecialchars($success_message); ?></p>
        <?php endif; ?>

        <h3>إضافة مجموعة قوالب عامة جديدة</h3>
        <form action="" method="post">
            <input type="hidden" name="action" value="add_global_template">
            <div class="form-group">
                <label>اسم مجموعة القوالب:</label>
                <input type="text" name="template_name" class="form-control" required>
            </div>
            <div class="form-group">
                <button type="submit" class="btn btn-primary">إضافة مجموعة قوالب</button>
            </div>
        </form>

        <h3>مجموعات القوالب العامة الموجودة</h3>
        <?php if (empty($templates)): ?>
            <p>لا توجد مجموعات قوالب عامة معرفة بعد. يرجى إضافة مجموعة.</p>
        <?php else: ?>
            <table>
                <thead>
                    <tr>
                        <th>اسم المجموعة</th>
                        <th>مفعلة</th>
                        <th>افتراضية</th>
                        <th>إجراءات</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($templates as $template): ?>
                        <tr>
                            <td><?php echo htmlspecialchars($template['template_name']); ?></td>
                            <td><?php echo $template['is_active'] ? 'نعم' : 'لا'; ?></td>
                            <td><?php echo $template['is_default'] ? 'نعم' : 'لا'; ?></td>
                            <td>
                                <form action="" method="post" style="display:inline;">
                                    <input type="hidden" name="action" value="edit_global_template">
                                    <input type="hidden" name="template_id" value="<?php echo $template['id']; ?>">
                                    <input type="text" name="template_name" value="<?php echo htmlspecialchars($template['template_name']); ?>" required style="width: 100px;">
                                    <label><input type="checkbox" name="is_active" <?php echo $template['is_active'] ? 'checked' : ''; ?>> تفعيل</label>
                                    <label><input type="checkbox" name="is_default" <?php echo $template['is_default'] ? 'checked' : ''; ?>> افتراضي</label>
                                    <button type="submit" class="btn btn-secondary">تحديث</button>
                                </form>
                                <a href="manage_global_template_replies.php?template_id=<?php echo $template['id']; ?>" class="btn btn-info">إدارة الردود</a>
                                <form action="" method="post" style="display:inline;" onsubmit="return confirm('هل أنت متأكد من حذف هذه المجموعة وجميع الردود المرتبطة بها؟');">
                                    <input type="hidden" name="action" value="delete_global_template">
                                    <input type="hidden" name="template_id" value="<?php echo $template['id']; ?>">
                                    <button type="submit" class="btn btn-danger">حذف</button>
                                </form>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                </tbody>
            </table>
        <?php endif; ?>
    </div>
</body>
</html>
