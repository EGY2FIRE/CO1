<?php
// manage_global_template_replies.php
// هذا الملف مسؤول عن إدارة الردود العشوائية لقالب عام محدد.

// 1. تضمين ملفات التحقق من المصادقة والإعدادات
require_once 'auth_check.php'; // يتحقق من تسجيل دخول المستخدم
require_once 'config.php';     // يحتوي على إعدادات قاعدة البيانات والفيسبوك

// الحصول على معرف المستخدم الحالي من الجلسة
$current_user_id = $_SESSION['user_id'];

// 2. التحقق من وجود معرف القالب في الرابط (GET parameter)
if (!isset($_GET['template_id']) || !is_numeric($_GET['template_id'])) {
    // إذا لم يتم توفير معرف القالب أو كان غير صالح، أعد التوجيه إلى صفحة القوالب العامة
    header("location: global_templates.php");
    exit();
}

$template_id = $_GET['template_id']; // معرف القالب العام في قاعدة البيانات المحلية

// 3. التحقق من أن القالب يتبع المستخدم الحالي
// هذا يمنع المستخدمين من الوصول إلى قوالب لا يملكونها
$stmt = $pdo->prepare("SELECT * FROM global_reply_templates WHERE id = :template_id AND user_id = :user_id");
$stmt->execute([':template_id' => $template_id, ':user_id' => $current_user_id]);
$global_template = $stmt->fetch();

// إذا لم يتم العثور على القالب أو كان لا يتبع المستخدم الحالي
if (!$global_template) {
    echo "القالب غير موجود أو لا تملك صلاحية الوصول إليه.";
    exit();
}

$error_message = ''; // تهيئة رسالة الخطأ

// 4. معالجة طلبات إضافة/تعديل/حذف محتوى الردود (POST requests)
if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    // إضافة محتوى رد جديد
    if (isset($_POST['action']) && $_POST['action'] == 'add_content') {
        $content = trim($_POST['content']);
        
        // التحقق من أن الحقل ليس فارغًا
        if (!empty($content)) {
            $stmt = $pdo->prepare("INSERT INTO global_reply_template_contents (template_id, content) VALUES (:template_id, :content)");
            $stmt->execute([':template_id' => $template_id, ':content' => $content]);
            
            // إعادة التوجيه لمنع إعادة إرسال النموذج عند تحديث الصفحة
            header("location: manage_global_template_replies.php?template_id=" . $template_id);
            exit();
        } else {
            $error_message = "الرجاء إدخال محتوى الرد.";
        }
    }

    // تعديل محتوى رد موجود
    if (isset($_POST['action']) && $_POST['action'] == 'edit_content') {
        $content_id = $_POST['content_id'];
        $content = trim($_POST['content']);
        
        // التحقق من أن الحقل ليس فارغًا
        if (!empty($content)) {
            // تحديث المحتوى في قاعدة البيانات، مع التأكد من أنه يتبع القالب الصحيح
            $stmt = $pdo->prepare("UPDATE global_reply_template_contents SET content = :content WHERE id = :content_id AND template_id = :template_id");
            $stmt->execute([
                ':content' => $content,
                ':content_id' => $content_id,
                ':template_id' => $template_id
            ]);
            
            // إعادة التوجيه لمنع إعادة إرسال النموذج
            header("location: manage_global_template_replies.php?template_id=" . $template_id);
            exit();
        } else {
            $error_message = "الرجاء إدخال محتوى الرد.";
        }
    }

    // حذف محتوى رد
    if (isset($_POST['action']) && $_POST['action'] == 'delete_content') {
        $content_id = $_POST['content_id'];
        
        // حذف المحتوى من قاعدة البيانات، مع التأكد من أنه يتبع القالب الصحيح
        $stmt = $pdo->prepare("DELETE FROM global_reply_template_contents WHERE id = :content_id AND template_id = :template_id");
        $stmt->execute([':content_id' => $content_id, ':template_id' => $template_id]);
        
        // إعادة التوجيه لمنع إعادة إرسال النموذج
        header("location: manage_global_template_replies.php?template_id=" . $template_id);
        exit();
    }
}

// 5. استرجاع محتويات الردود للقالب الحالي لعرضها في الواجهة
$stmt = $pdo->prepare("SELECT * FROM global_reply_template_contents WHERE template_id = :template_id ORDER BY id DESC");
$stmt->execute([':template_id' => $template_id]);
$contents = $stmt->fetchAll();
?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>إدارة الردود لـ "<?php echo htmlspecialchars($global_template['template_name']); ?>"</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <?php include 'navbar.php'; // تضمين شريط التنقل العلوي ?>
    <div class="wrapper">
        <h2>إدارة الردود لمجموعة القوالب: "<?php echo htmlspecialchars($global_template['template_name']); ?>"</h2>
        <a href="global_templates.php" class="btn btn-secondary">العودة إلى القوالب العامة</a>

        <?php if (isset($error_message) && !empty($error_message)): ?>
            <p style="color: red; font-weight: bold;"><?php echo htmlspecialchars($error_message); ?></p>
        <?php endif; ?>

        <h3>إضافة رد جديد</h3>
        <form action="" method="post">
            <input type="hidden" name="action" value="add_content">
            <div class="form-group">
                <label>محتوى الرد (استخدم {user_name} لاسم المعلق):</label>
                <textarea name="content" class="form-control" rows="3" required></textarea>
            </div>
            <div class="form-group">
                <button type="submit" class="btn btn-primary">إضافة رد</button>
            </div>
        </form>

        <h3>الردود الموجودة</h3>
        <?php if (empty($contents)): ?>
            <p>لا توجد ردود معرفة لهذه المجموعة بعد. يرجى إضافة رد.</p>
        <?php else: ?>
            <table>
                <thead>
                    <tr>
                        <th>المحتوى</th>
                        <th>إجراءات</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($contents as $content_item): ?>
                        <tr>
                            <td><?php echo htmlspecialchars($content_item['content']); ?></td>
                            <td>
                                <form action="" method="post" style="display:inline;">
                                    <input type="hidden" name="action" value="edit_content">
                                    <input type="hidden" name="content_id" value="<?php echo $content_item['id']; ?>">
                                    <textarea name="content" rows="2" required style="width: 300px;"><?php echo htmlspecialchars($content_item['content']); ?></textarea>
                                    <button type="submit" class="btn btn-secondary">تحديث</button>
                                </form>
                                <form action="" method="post" style="display:inline;" onsubmit="return confirm('هل أنت متأكد من حذف هذا الرد؟');">
                                    <input type="hidden" name="action" value="delete_content">
                                    <input type="hidden" name="content_id" value="<?php echo $content_item['id']; ?>">
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
