<?php
require_once 'auth_check.php';
require_once 'config.php';
require_once 'helpers.php'; // تضمين ملف الدوال المساعدة

if (!isset($_GET['page_id']) || !is_numeric($_GET['page_id'])) {
    header("location: pages.php");
    exit();
}

$page_id = $_GET['page_id'];
$current_user_id = $_SESSION['user_id'];

// التحقق من أن الصفحة تابعة للمستخدم
// **** تم إضافة current_active_account_id و last_rotation_start_time لجلب معلومات الدوران ****
$stmtPage = $pdo->prepare("SELECT id, page_name, page_url, user_id, current_active_account_id, last_rotation_start_time FROM facebook_pages WHERE id = :page_id AND user_id = :user_id");
$stmtPage->execute([':page_id' => $page_id, ':user_id' => $current_user_id]);
$page = $stmtPage->fetch();

if (!$page) {
    echo "الصفحة غير موجودة أو لا تملك صلاحية الوصول إليها.";
    exit();
}

// معالجة طلبات تحديث إعدادات الحساب
if ($_SERVER['REQUEST_METHOD'] == 'POST' && isset($_POST['action']) && $_POST['action'] == 'update_account_settings') {
    $account_db_id = $_POST['account_db_id'];

    // التأكد أن هذا الربط يخص المستخدم الحالي
    $stmtCheckOwner = $pdo->prepare("SELECT id FROM facebook_page_accounts WHERE id = :id AND user_id = :user_id");
    $stmtCheckOwner->execute([':id' => $account_db_id, ':user_id' => $current_user_id]);
    if ($stmtCheckOwner->rowCount() == 0) {
        echo "خطأ: لا تملك صلاحية لتعديل هذا الحساب.";
        exit();
    }

    $interval = intval($_POST['reply_interval_seconds']);
    $limit = intval($_POST['comments_limit_per_run']);
    $custom_since = null;
    if (isset($_POST['custom_since_hours']) && is_numeric($_POST['custom_since_hours']) && $_POST['custom_since_hours'] >= 0) {
        $custom_since = intval($_POST['custom_since_hours']);
    }
    $notes = trim($_POST['notes']);
    $enable_reactions = isset($_POST['enable_reactions']) ? 1 : 0;
    $reaction_type = trim($_POST['reaction_type']);
    $rotation_duration = intval($_POST['rotation_duration_minutes']);
    $max_replies_per_hour = intval($_POST['max_replies_per_hour']);

    $stmt = $pdo->prepare("UPDATE facebook_page_accounts SET 
        reply_interval_seconds = :interval, 
        comments_limit_per_run = :limit, 
        custom_since_hours = :custom_since,
        notes = :notes,
        enable_reactions = :enable_reactions,
        reaction_type = :reaction_type,
        rotation_duration_minutes = :rotation_duration,
        max_replies_per_hour = :max_replies_per_hour
        WHERE id = :id");
    $stmt->execute([
        ':interval' => $interval, 
        ':limit' => $limit, 
        ':custom_since' => $custom_since,
        ':notes' => $notes,
        ':enable_reactions' => $enable_reactions,
        ':reaction_type' => $reaction_type,
        ':rotation_duration' => $rotation_duration,
        ':max_replies_per_hour' => $max_replies_per_hour,
        ':id' => $account_db_id
    ]);
    header("location: page_details.php?page_id=" . $page_id); // إعادة توجيه بعد التحديث
    exit();
}

// استرجاع جميع الحسابات المرتبطة بهذه الصفحة
$stmtAccounts = $pdo->prepare("SELECT fpa.* FROM facebook_page_accounts fpa WHERE fpa.page_id = :page_id AND fpa.user_id = :user_id ORDER BY fpa.account_name");
$stmtAccounts->execute([':page_id' => $page_id, ':user_id' => $current_user_id]);
$accounts = $stmtAccounts->fetchAll();

// دالة لحساب عدد الردود في فترة زمنية معينة (تستخدم الآن توقيت UTC)
function get_replies_count($pdo, $account_id, $hours) {
    $start_time_utc = (new DateTime('now', new DateTimeZone('UTC')))->modify("-{$hours} hours")->format('Y-m-d H:i:s');
    $stmt = $pdo->prepare("SELECT SUM(reply_count) FROM hourly_reply_stats WHERE fb_page_account_id = :account_id AND hour_timestamp >= :start_time_utc");
    $stmt->execute([':account_id' => $account_id, ':start_time_utc' => $start_time_utc]);
    return $stmt->fetchColumn() ?? 0;
}

?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>تفاصيل الصفحة: <?php echo htmlspecialchars($page['page_name']); ?></title>
    <link rel="stylesheet" href="style.css">
    <style>
        .page-details-header {
            background-color: #e9ecef;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        .page-details-header h2 {
            margin-top: 0;
            color: #007bff;
        }
        .account-block {
            border: 1px solid #ccc;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 8px;
            background-color: #f9f9f9;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .account-block h4 {
            margin-top: 0;
            color: #343a40;
        }
        .status-success { color: green; font-weight: bold; }
        .status-error { color: red; font-weight: bold; }
        .status-no_new_comments { color: orange; font-weight: bold; }
        .status-never_run { color: gray; font-weight: bold; }
        .limit_reached { background-color: #f8d7da; color: #721c24; } /* أحمر فاتح للحد الأقصى */
        .error-details {
            background-color: #ffe0e0;
            border: 1px solid red;
            padding: 8px;
            margin-top: 5px;
            border-radius: 4px;
            font-size: 0.9em;
            word-break: break-all;
        }
        .form-group label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        .form-control {
            width: 100%;
            padding: 8px;
            border: 1px solid #ccc;
            border-radius: 4px;
            box-sizing: border-box;
        }
        .btn-sm {
            padding: 5px 10px;
            font-size: 0.8em;
        }
        .stats-section {
            margin-top: 15px;
            padding-top: 10px;
            border-top: 1px dashed #eee;
        }
    </style>
</head>
<body>
    <?php include 'navbar.php'; ?>
    <div class="wrapper">
        <div class="page-details-header">
            <h2>تفاصيل الصفحة: <a href="<?php echo htmlspecialchars($page['page_url']); ?>" target="_blank"><?php echo htmlspecialchars($page['page_name']); ?></a></h2>
            <p><a href="pages.php" class="btn btn-secondary">العودة إلى الصفحات</a></p>
            <a href="edit_templates.php?page_id=<?php echo $page['id']; ?>" class="btn btn-info">إدارة قوالب الردود للصفحة</a>
            
            <!-- **** عرض معلومات الحساب النشط حالياً للصفحة **** -->
            <?php
            $current_active_account_name = 'غير محدد';
            if (!empty($page['current_active_account_id'])) {
                $stmtCurrentActiveAccount = $pdo->prepare("SELECT account_name FROM facebook_page_accounts WHERE id = :id");
                $stmtCurrentActiveAccount->execute([':id' => $page['current_active_account_id']]);
                $active_acc_info = $stmtCurrentActiveAccount->fetch();
                if ($active_acc_info) {
                    $current_active_account_name = $active_acc_info['account_name'];
                }
            }
            ?>
            <p><strong>الحساب النشط حالياً للصفحة:</strong> <?php echo htmlspecialchars($current_active_account_name); ?></p>
            <?php if (!empty($page['last_rotation_start_time'])): 
                $rotation_start_dt_utc = new DateTime($page['last_rotation_start_time'], new DateTimeZone('UTC'));
                $rotation_start_dt_cairo = clone $rotation_start_dt_utc;
                $rotation_start_dt_cairo->setTimezone(new DateTimeZone('Africa/Cairo'));
            ?>
                <p><strong>وقت بداية دورة الحساب الحالي:</strong> <?php echo htmlspecialchars($rotation_start_dt_cairo->format('Y-m-d H:i:s')); ?> (<?php echo time_ago_in_arabic($rotation_start_dt_cairo->format('Y-m-d H:i:s')); ?>)</p>
            <?php endif; ?>
            <!-- ********************************************** -->

        </div>

        <?php if (empty($accounts)): ?>
            <p>لا توجد حسابات مرتبطة بهذه الصفحة.</p>
            <a href="add_page.php" class="btn btn-primary">ربط حساب جديد بهذه الصفحة</a>
        <?php else: ?>
            <?php foreach ($accounts as $account): ?>
                <div class="account-block">
                    <h4>
                        <a href="https://www.facebook.com/profile.php?id=<?php echo htmlspecialchars($account['account_id_fb']); ?>" target="_blank">
                            <?php echo htmlspecialchars($account['account_name']); ?>
                        </a>
                    </h4>
                    <p>حالة الرد التلقائي:
                        <form method="post" action="pages.php" style="display:inline;">
                            <input type="hidden" name="action" value="toggle_account_active">
                            <input type="hidden" name="account_db_id" value="<?php echo $account['id']; ?>">
                            <input type="hidden" name="current_status" value="<?php echo $account['is_active'] ? 'true' : 'false'; ?>">
                            <button type="submit" class="btn <?php echo $account['is_active'] ? 'btn-success' : 'btn-danger'; ?> btn-sm">
                                <?php echo $account['is_active'] ? 'مفعل' : 'معطل'; ?>
                            </button>
                        </form>
                    </p>
                    <p>
                        <strong>حالة آخر تشغيل: </strong> 
                        <span class="status-<?php echo htmlspecialchars($account['last_run_status']); ?>">
                            <?php 
                                if ($account['last_run_status'] == 'success') echo 'نجاح';
                                elseif ($account['last_run_status'] == 'error') echo 'خطأ';
                                elseif ($account['last_run_status'] == 'no_new_comments') echo 'لا تعليقات جديدة';
                                elseif ($account['last_run_status'] == 'limit_reached') echo 'وصل للحد الأقصى'; // حالة جديدة
                                else echo 'لم يعمل بعد';
                            ?>
                        </span>
                        <?php if (!empty($account['last_run_message'])): ?>
                            - <?php echo htmlspecialchars($account['last_run_message']); ?>
                        <?php endif; ?>
                    </p>
                    <?php if (!empty($account['last_error_message'])): ?>
                        <div class="error-details">
                            <strong style="color: red;">رسالة الخطأ:</strong> <?php echo htmlspecialchars($account['last_error_message']); ?>
                        </div>
                    <?php endif; ?>
                    <p>
                        <strong>آخر تعليق تم الرد عليه:</strong> 
                        <?php 
                            if (!empty($account['last_processed_comment_time']) && $account['last_processed_comment_time'] != '1970-01-01 00:00:00') {
                                $utc_time = new DateTime($account['last_processed_comment_time'], new DateTimeZone('UTC'));
                                $cairo_time = clone $utc_time;
                                $cairo_time->setTimezone(new DateTimeZone('Africa/Cairo'));
                                echo htmlspecialchars($cairo_time->format('Y-m-d H:i:s')) . ' (' . time_ago_in_arabic($cairo_time->format('Y-m-d H:i:s')) . ')';
                            } else {
                                echo 'لا يوجد';
                            }
                        ?>
                    </p>
                    <p>تاريخ انتهاء التوكن (تقريبي): 
                        <?php 
                            if (!empty($account['token_expiry_date'])) {
                                $utc_expiry_time = new DateTime($account['token_expiry_date'], new DateTimeZone('UTC'));
                                $cairo_expiry_time = clone $utc_expiry_time;
                                $cairo_expiry_time->setTimezone(new DateTimeZone('Africa/Cairo'));
                                echo htmlspecialchars($cairo_expiry_time->format('Y-m-d H:i:s'));
                            } else {
                                echo 'غير محدد';
                            }
                        ?>
                    </p>

                    <div class="stats-section">
                        <h4>إحصائيات الردود:</h4>
                        <p>الردود في آخر ساعة: <strong><?php echo get_replies_count($pdo, $account['id'], 1); ?></strong></p>
                        <p>الردود في آخر 24 ساعة: <strong><?php echo get_replies_count($pdo, $account['id'], 24); ?></strong></p>
                        <a href="account_stats.php?account_id=<?php echo $account['id']; ?>" class="btn btn-info btn-sm">إحصائيات مفصلة</a>
                    </div>

                    <form method="post" action="page_details.php?page_id=<?php echo $page_id; ?>">
                        <input type="hidden" name="action" value="update_account_settings">
                        <input type="hidden" name="account_db_id" value="<?php echo $account['id']; ?>">
                        <div class="form-group">
                            <label>مدة تشغيل السكريبت (ثوانٍ بين الردود):</label>
                            <input type="number" name="reply_interval_seconds" value="<?php echo htmlspecialchars($account['reply_interval_seconds']); ?>" min="1" class="form-control" style="width: 100px; display: inline-block;">
                        </div>
                        <div class="form-group">
                            <label>عدد التعليقات في كل تشغيل:</label>
                            <input type="number" name="comments_limit_per_run" value="<?php echo htmlspecialchars($account['comments_limit_per_run']); ?>" min="1" class="form-control" style="width: 100px; display: inline-block;">
                        </div>
                        <div class="form-group">
                            <label>مدة البحث عن التعليقات (بالساعات، فارغ للافتراضي 24 ساعة):</label>
                            <input type="number" name="custom_since_hours" value="<?php echo htmlspecialchars($account['custom_since_hours'] ?? ''); ?>" min="0" class="form-control" placeholder="24" style="width: 100px; display: inline-block;">
                        </div>
                        <div class="form-group">
                            <label>مدة دورة الحساب (بالدقائق):</label>
                            <input type="number" name="rotation_duration_minutes" value="<?php echo htmlspecialchars($account['rotation_duration_minutes']); ?>" min="1" class="form-control" style="width: 100px; display: inline-block;">
                        </div>
                        <div class="form-group">
                            <label>الحد الأقصى للردود في الساعة:</label>
                            <input type="number" name="max_replies_per_hour" value="<?php echo htmlspecialchars($account['max_replies_per_hour']); ?>" min="0" class="form-control" style="width: 100px; display: inline-block;">
                        </div>
                        <div class="form-group">
                            <label>تفعيل الإعجاب بالتعليقات:</label>
                            <input type="checkbox" name="enable_reactions" value="1" <?php echo $account['enable_reactions'] ? 'checked' : ''; ?>>
                        </div>
                        <div class="form-group">
                            <label>نوع الإعجاب:</label>
                            <select name="reaction_type" class="form-control" style="width: 150px; display: inline-block;">
                                <option value="LIKE" <?php echo ($account['reaction_type'] == 'LIKE') ? 'selected' : ''; ?>>أعجبني</option>
                                <option value="LOVE" <?php echo ($account['reaction_type'] == 'LOVE') ? 'selected' : ''; ?>>أحببته</option>
                                <option value="CARE" <?php echo ($account['reaction_type'] == 'CARE') ? 'selected' : ''; ?>>أدعمه</option>
                                <option value="HAHA" <?php echo ($account['reaction_type'] == 'HAHA') ? 'selected' : ''; ?>>هاها</option>
                                <option value="WOW" <?php echo ($account['reaction_type'] == 'WOW') ? 'selected' : ''; ?>>واو</option>
                                <option value="SAD" <?php echo ($account['reaction_type'] == 'SAD') ? 'selected' : ''; ?>>حزين</option>
                                <option value="ANGRY" <?php echo ($account['reaction_type'] == 'ANGRY') ? 'selected' : ''; ?>>غاضب</option>
                            </select>
                        </div>
                        <div class="form-group">
                            <label>ملاحظات سريعة:</label>
                            <textarea name="notes" class="form-control" rows="2"><?php echo htmlspecialchars($account['notes'] ?? ''); ?></textarea>
                        </div>
                        <button type="submit" class="btn btn-primary">تحديث إعدادات الحساب</button>
                    </form>
                    <!-- **** زر التشغيل اللحظي **** -->
                    <form action="run_account_now.php" method="get" style="display:inline;" onsubmit="alert('سيتم تشغيل الحساب الآن. تحقق من سجل manual_run_activity.log للمتابعة.');">
                        <input type="hidden" name="account_id" value="<?php echo $account['id']; ?>">
                        <button type="submit" class="btn btn-success">تشغيل الآن (اختبار)</button>
                    </form>
                    <!-- ************************** -->
                    <form method="post" action="pages.php" style="display:inline;" onsubmit="return confirm('هل أنت متأكد من حذف هذا الارتباط للحساب (<?php echo htmlspecialchars($account['account_name']); ?>) بالصفحة (<?php echo htmlspecialchars($page['page_name']); ?>)؟');">
                        <input type="hidden" name="action" value="delete_account_link">
                        <input type="hidden" name="account_db_id" value="<?php echo $account['id']; ?>">
                        <button type="submit" class="btn btn-danger">حذف ربط الحساب</button>
                    </form>
                </div>
            <?php endforeach; ?>
        <?php endif; ?>
    </div>
</body>
</html>
