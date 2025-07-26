<?php
// global_templates.php
// هذا الملف مسؤول عن إدارة قوالب الردود العامة للمستخدم الحالي.

// 1. تضمين ملفات التحقق من المصادقة والإعدادات
require_once 'auth_check.php'; // يتحقق من تسجيل دخول المستخدم
require_once 'config.php';     // يحتوي على إعدادات قاعدة البيانات والفيسبوك

// الحصول على معرف المستخدم الحالي من الجلسة
$current_user_id = $_SESSION['user_id'];

// 2. معالجة طلبات إضافة/تعديل/حذف القوالب العامة (POST requests)
if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    // إضافة قالب عام جديد
    if (isset($_POST['action']) && $_POST['action'] == 'add_global_template') {
        $template_name = trim($_POST['template_name']);
        $template_content = trim($_POST['template_content']);
        
        // التحقق من أن الحقول ليست فارغة
        if (!empty($template_name) && !empty($template_content)) {
            try {
                $stmt = $pdo->prepare("INSERT INTO global_reply_templates (user_id, template_name, template_content) VALUES (:user_id, :template_name, :template_content)");
                $stmt->execute([
                    ':user_id' => $current_user_id,
                    ':template_name' => $template_name,
                    ':template_content' => $template_content
                ]);
                // إعادة التوجيه لمنع إعادة إرسال النموذج عند تحديث الصفحة
                header("location: global_templates.php");
                exit();
            } catch (PDOException $e) {
                // التعامل مع خطأ فريد (مثل تكرار اسم القالب)
                if ($e->getCode() == 23000) { // SQLSTATE for integrity constraint violation
                    $error_message = "اسم القالب هذا موجود بالفعل. يرجى اختيار اسم آخر.";
                } else {
                    $error_message = "حدث خطأ أثناء إضافة القالب: " . $e->getMessage();
                }
            }
        } else {
            $error_message = "الرجاء ملء جميع الحقول المطلوبة.";
        }
    }

    // تعديل قالب عام موجود
    if (isset($_POST['action']) && $_POST['action'] == 'edit_global_template') {
        $template_id = $_POST['template_id'];
        $template_name = trim($_POST['template_name']);
        $template_content = trim($_POST['template_content']);
        $is_active = isset($_POST['is_active']) ? 1 : 0; // تحويل قيمة checkbox إلى 0 أو 1
        
        // التحقق من أن الحقول ليست فارغة
        if (!empty($template_name) && !empty($template_content)) {
            try {
                // تحديث القالب في قاعدة البيانات، مع التأكد من أنه يتبع المستخدم الصحيح
                $stmt = $pdo->prepare("UPDATE global_reply_templates SET template_name = :template_name, template_content = :template_content, is_active = :is_active WHERE id = :template_id AND user_id = :user_id");
                $stmt->execute([
                    ':template_name' => $template_name,
                    ':template_content' => $template_content,
                    ':is_active' => $is_active,
                    ':template_id' => $template_id,
                    ':user_id' => $current_user_id
                ]);
                // إعادة التوجيه لمنع إعادة إرسال النموذج
                header("location: global_templates.php");
                exit();
            } catch (PDOException $e) {
                if ($e->getCode() == 23000) {
                    $error_message = "اسم القالب هذا موجود بالفعل. يرجى اختيار اسم آخر.";
                } else {
                    $error_message = "حدث خطأ أثناء تحديث القالب: " . $e->getMessage();
                }
            }
        } else {
            $error_message = "الرجاء ملء جميع الحقول المطلوبة.";
        }
    }

    // حذف قالب عام
    if (isset($_POST['action']) && $_POST['action'] == 'delete_global_template') {
        $template_id = $_POST['template_id'];
        
        // حذف القالب من قاعدة البيانات، مع التأكد من أنه يتبع المستخدم الصحيح
        $stmt = $pdo->prepare("DELETE FROM global_reply_templates WHERE id = :template_id AND user_id = :user_id");
        $stmt->execute([':template_id' => $template_id, ':user_id' => $current_user_id]);
        
        // إعادة التوجيه لمنع إعادة إرسال النموذج
        header("location: global_templates.php");
        exit();
    }
}

// 3. استرجاع قوالب الردود العامة للمستخدم الحالي لعرضها في الواجهة
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
        <p>هذه القوالب ستستخدم للرد على الصفحات التي ليس لها قوالب ردود مخصصة.</p>
        
        <?php if (isset($error_message)): ?>
            <p style="color: red; font-weight: bold;"><?php echo htmlspecialchars($error_message); ?></p>
        <?php endif; ?>

        <h3>إضافة قالب عام جديد</h3>
        <form action="" method="post">
            <input type="hidden" name="action" value="add_global_template">
            <div class="form-group">
                <label>اسم القالب:</label>
                <input type="text" name="template_name" class="form-control" required>
            </div>
            <div class="form-group">
                <label>محتوى الرد (استخدم {user_name} لاسم المعلق):</label>
                <textarea name="template_content" class="form-control" rows="5" required></textarea>
            </div>
            <div class="form-group">
                <button type="submit" class="btn btn-primary">إضافة قالب عام</button>
            </div>
        </form>

        <h3>القوالب العامة الموجودة</h3>
        <?php if (empty($templates)): ?>
            <p>لا توجد قوالب عامة معرفة بعد. يرجى إضافة قالب.</p>
        <?php else: ?>
            <table>
                <thead>
                    <tr>
                        <th>اسم القالب</th>
                        <th>المحتوى</th>
                        <th>مفعل</th>
                        <th>إجراءات</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($templates as $template): ?>
                        <tr>
                            <td><?php echo htmlspecialchars($template['template_name']); ?></td>
                            <td><?php echo htmlspecialchars($template['template_content']); ?></td>
                            <td><?php echo $template['is_active'] ? 'نعم' : 'لا'; ?></td>
                            <td>
                                <form action="" method="post" style="display:inline;">
                                    <input type="hidden" name="action" value="edit_global_template">
                                    <input type="hidden" name="template_id" value="<?php echo $template['id']; ?>">
                                    <input type="text" name="template_name" value="<?php echo htmlspecialchars($template['template_name']); ?>" required style="width: 100px;">
                                    <textarea name="template_content" rows="2" required style="width: 200px;"><?php echo htmlspecialchars($template['template_content']); ?></textarea>
                                    <label><input type="checkbox" name="is_active" <?php echo $template['is_active'] ? 'checked' : ''; ?>> تفعيل</label>
                                    <button type="submit" class="btn btn-secondary">تحديث</button>
                                </form>
                                <form action="" method="post" style="display:inline;" onsubmit="return confirm('هل أنت متأكد من حذف هذا القالب العام؟');">
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
