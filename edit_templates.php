<?php
// edit_templates.php
// هذا الملف مسؤول عن إدارة أسماء قوالب الردود المخصصة لصفحة فيسبوك محددة.

// 1. تضمين ملفات التحقق من المصادقة والإعدادات
require_once 'auth_check.php'; // يتحقق من تسجيل دخول المستخدم
require_once 'config.php';     // يحتوي على إعدادات قاعدة البيانات والفيسبوك

// 2. التحقق من وجود معرف الصفحة في الرابط (GET parameter)
if (!isset($_GET['page_id']) || !is_numeric($_GET['page_id'])) {
    // إذا لم يتم توفير معرف الصفحة أو كان غير صالح، أعد التوجيه إلى صفحة الصفحات
    header("location: pages.php");
    exit();
}

$page_id = $_GET['page_id']; // معرف الصفحة في قاعدة البيانات المحلية

// 3. التحقق من أن الصفحة تابعة للمستخدم الحالي
// هذا يمنع المستخدمين من الوصول إلى صفحات لا يملكونها
$stmt = $pdo->prepare("SELECT * FROM facebook_pages WHERE id = :page_id AND user_id = :user_id");
$stmt->execute([':page_id' => $page_id, ':user_id' => $_SESSION['user_id']]);
$page = $stmt->fetch();

// إذا لم يتم العثور على الصفحة أو كانت لا تتبع المستخدم الحالي
if (!$page) {
    echo "الصفحة غير موجودة أو لا تملك صلاحية الوصول إليها.";
    exit();
}

$error_message = ''; // تهيئة رسالة الخطأ

// 4. معالجة طلبات إضافة/تعديل/حذف أسماء القوالب المخصصة (POST requests)
if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    // إضافة قالب جديد
    if (isset($_POST['action']) && $_POST['action'] == 'add_template') {
        $template_name = trim($_POST['template_name']);
        
        // التحقق من أن الحقل ليس فارغًا
        if (!empty($template_name)) {
            try {
                $stmt = $pdo->prepare("INSERT INTO reply_templates (page_id, template_name) VALUES (:page_id, :template_name)");
                $stmt->execute([':page_id' => $page_id, ':template_name' => $template_name]);
                
                // إعادة التوجيه لمنع إعادة إرسال النموذج عند تحديث الصفحة
                header("location: edit_templates.php?page_id=" . $page_id);
                exit();
            } catch (PDOException $e) {
                if ($e->getCode() == 23000) {
                    $error_message = "اسم القالب هذا موجود بالفعل لهذه الصفحة. يرجى اختيار اسم آخر.";
                } else {
                    $error_message = "حدث خطأ أثناء إضافة القالب: " . $e->getMessage();
                }
            }
        } else {
            $error_message = "الرجاء إدخال اسم القالب.";
        }
    }

    // تعديل قالب موجود (اسم القالب أو حالة التفعيل)
    if (isset($_POST['action']) && $_POST['action'] == 'edit_template') {
        $template_id = $_POST['template_id'];
        $template_name = trim($_POST['template_name']);
        $is_active = isset($_POST['is_active']) ? 1 : 0; // تحويل قيمة checkbox إلى 0 أو 1
        
        // التحقق من أن الحقل ليس فارغًا
        if (!empty($template_name)) {
            try {
                // تحديث القالب في قاعدة البيانات، مع التأكد من أنه يتبع الصفحة الصحيحة
                $stmt = $pdo->prepare("UPDATE reply_templates SET template_name = :template_name, is_active = :is_active WHERE id = :template_id AND page_id = :page_id");
                $stmt->execute([
                    ':template_name' => $template_name,
                    ':is_active' => $is_active,
                    ':template_id' => $template_id,
                    ':page_id' => $page_id
                ]);
                
                // إعادة التوجيه لمنع إعادة إرسال النموذج
                header("location: edit_templates.php?page_id=" . $page_id);
                exit();
            } catch (PDOException $e) {
                if ($e->getCode() == 23000) {
                    $error_message = "اسم القالب هذا موجود بالفعل لهذه الصفحة. يرجى اختيار اسم آخر.";
                } else {
                    $error_message = "حدث خطأ أثناء تحديث القالب: " . $e->getMessage();
                }
            }
        } else {
            $error_message = "الرجاء إدخال اسم القالب.";
        }
    }

    // حذف قالب
    if (isset($_POST['action']) && $_POST['action'] == 'delete_template') {
        $template_id = $_POST['template_id'];
        
        // حذف القالب من قاعدة البيانات، مع التأكد من أنه يتبع الصفحة الصحيحة
        // سيتم حذف جميع الردود المرتبطة به تلقائيًا بسبب ON DELETE CASCADE
        $stmt = $pdo->prepare("DELETE FROM reply_templates WHERE id = :template_id AND page_id = :page_id");
        $stmt->execute([':template_id' => $template_id, ':page_id' => $page_id]);
        
        // إعادة التوجيه لمنع إعادة إرسال النموذج
        header("location: edit_templates.php?page_id=" . $page_id);
        exit();
    }
}

// 5. استرجاع أسماء القوالب المخصصة للصفحة لعرضها في الواجهة
$stmt = $pdo->prepare("SELECT * FROM reply_templates WHERE page_id = :page_id ORDER BY id DESC");
$stmt->execute([':page_id' => $page_id]);
$templates = $stmt->fetchAll();
?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>إدارة قوالب الردود لـ <?php echo htmlspecialchars($page['page_name']); ?></title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <?php include 'navbar.php'; // تضمين شريط التنقل العلوي ?>
    <div class="wrapper">
        <h2>إدارة قوالب الردود لـ "<?php echo htmlspecialchars($page['page_name']); ?>"</h2>
        <a href="pages.php" class="btn btn-secondary">العودة إلى الصفحات</a>

        <?php if (isset($error_message) && !empty($error_message)): ?>
            <p style="color: red; font-weight: bold;"><?php echo htmlspecialchars($error_message); ?></p>
        <?php endif; ?>

        <h3>إضافة مجموعة قوالب رد جديدة</h3>
        <form action="" method="post">
            <input type="hidden" name="action" value="add_template">
            <div class="form-group">
                <label>اسم مجموعة القوالب:</label>
                <input type="text" name="template_name" class="form-control" required>
            </div>
            <div class="form-group">
                <button type="submit" class="btn btn-primary">إضافة مجموعة قوالب</button>
            </div>
        </form>

        <h3>مجموعات القوالب المخصصة الموجودة</h3>
        <?php if (empty($templates)): ?>
            <p>لا توجد مجموعات قوالب ردود لهذه الصفحة بعد.</p>
        <?php else: ?>
            <table>
                <thead>
                    <tr>
                        <th>اسم المجموعة</th>
                        <th>مفعلة</th>
                        <th>إجراءات</th>
                    </tr>
                </thead>
                <tbody>
                    <?php foreach ($templates as $template): ?>
                        <tr>
                            <td><?php echo htmlspecialchars($template['template_name']); ?></td>
                            <td><?php echo $template['is_active'] ? 'نعم' : 'لا'; ?></td>
                            <td>
                                <form action="" method="post" style="display:inline;">
                                    <input type="hidden" name="action" value="edit_template">
                                    <input type="hidden" name="template_id" value="<?php echo $template['id']; ?>">
                                    <input type="text" name="template_name" value="<?php echo htmlspecialchars($template['template_name']); ?>" required style="width: 100px;">
                                    <label><input type="checkbox" name="is_active" <?php echo $template['is_active'] ? 'checked' : ''; ?>> تفعيل</label>
                                    <button type="submit" class="btn btn-secondary">تحديث</button>
                                </form>
                                <a href="manage_page_template_replies.php?template_id=<?php echo $template['id']; ?>" class="btn btn-info">إدارة الردود</a>
                                <form action="" method="post" style="display:inline;" onsubmit="return confirm('هل أنت متأكد من حذف هذه المجموعة وجميع الردود المرتبطة بها؟');">
                                    <input type="hidden" name="action" value="delete_template">
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
