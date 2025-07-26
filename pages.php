<?php
require_once 'auth_check.php';
require_once 'config.php';
require_once 'helpers.php'; // تضمين ملف الدوال المساعدة

$search_query = isset($_GET['search']) ? trim($_GET['search']) : '';
$sql_search_condition = '';
if (!empty($search_query)) {
    $sql_search_condition = " AND (fp.page_name LIKE :search_query OR fpa.account_name LIKE :search_query) ";
}

// معالجة طلبات الحذف/التعديل (جزئي - فقط تفعيل/تعطيل الحسابات)
if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    if (isset($_POST['action']) && isset($_POST['account_db_id'])) {
        $account_db_id = $_POST['account_db_id'];

        // التأكد أن هذا الربط (account_db_id) يخص المستخدم الحالي
        $stmtCheckOwner = $pdo->prepare("SELECT id FROM facebook_page_accounts WHERE id = :id AND user_id = :user_id");
        $stmtCheckOwner->execute([':id' => $account_db_id, ':user_id' => $_SESSION['user_id']]);
        if ($stmtCheckOwner->rowCount() == 0) {
            echo "خطأ: لا تملك صلاحية لتعديل هذا الحساب.";
            exit();
        }

        if ($_POST['action'] == 'delete_account_link') {
            $stmt = $pdo->prepare("DELETE FROM facebook_page_accounts WHERE id = :id");
            $stmt->execute([':id' => $account_db_id]);
            header("location: pages.php?search=" . urlencode($search_query));
            exit();
        } elseif ($_POST['action'] == 'toggle_account_active') {
            $current_status = filter_var($_POST['current_status'], FILTER_VALIDATE_BOOLEAN);
            $new_status = !$current_status;
            $stmt = $pdo->prepare("UPDATE facebook_page_accounts SET is_active = :is_active WHERE id = :id");
            $stmt->execute([':is_active' => $new_status, ':id' => $account_db_id]);
            header("location: pages.php?search=" . urlencode($search_query));
            exit();
        }
    }
}

// استرجاع الصفحات والحسابات المرتبطة بها لغرض العرض والترتيب
$stmt = $pdo->prepare("SELECT 
                            fp.id AS page_db_id, fp.page_id_fb, fp.page_name, fp.page_url,
                            fp.current_active_account_id, fp.last_rotation_start_time, -- **** تم إضافة هذه الأعمدة ****
                            fpa.id AS account_db_id, fpa.account_name, fpa.token_expiry_date, fpa.is_active,
                            fpa.last_run_status, fpa.last_run_message, fpa.last_error_message
                       FROM facebook_pages fp
                       JOIN facebook_page_accounts fpa ON fp.id = fpa.page_id
                       WHERE fp.user_id = :user_id" . $sql_search_condition . "
                       ORDER BY fp.page_name, fpa.account_name");

$params = [':user_id' => $_SESSION['user_id']];
if (!empty($search_query)) {
    $params[':search_query'] = '%' . $search_query . '%';
}
$stmt->execute($params);
$raw_data = $stmt->fetchAll();

// تجميع البيانات في هيكل هرمي: صفحة -> حسابات
$pages_structured = [];
foreach ($raw_data as $row) {
    $page_id_from_db = $row['page_db_id']; // الحصول على الـ ID الصحيح من قاعدة البيانات
    
    if (!isset($pages_structured[$page_id_from_db])) {
        $pages_structured[$page_id_from_db] = [
            'page_name' => $row['page_name'],
            'page_url' => $row['page_url'],
            'page_db_id_for_display' => $row['page_db_id'], // هذا الحقل تم الاحتفاظ به للعرض الصحيح
            'current_active_account_id' => $row['current_active_account_id'], // **** تم إضافة هذا الحقل ****
            'last_rotation_start_time' => $row['last_rotation_start_time'],   // **** تم إضافة هذا الحقل ****
            'accounts' => []    
        ];
    }
    // إضافة معلومات الحساب إلى قائمة الحسابات لهذه الصفحة
    $pages_structured[$page_id_from_db]['accounts'][] = [
        'account_db_id' => $row['account_db_id'],
        'account_name' => $row['account_name'],
        'token_expiry_date' => $row['token_expiry_date'],
        'is_active' => $row['is_active'],
        'last_run_status' => $row['last_run_status'],
        'last_run_message' => $row['last_run_message'],
        'last_error_message' => $row['last_error_message']
    ];
}

// **** منطق الترتيب ****
$sort_by = $_GET['sort_by'] ?? 'page_name'; // الافتراضي: الترتيب باسم الصفحة
$sort_order = $_GET['sort_order'] ?? 'ASC'; // الافتراضي: تصاعدي

// ترتيب الصفحات الرئيسية
usort($pages_structured, function($a, $b) use ($sort_by, $sort_order) {
    // نستخدم $a['page_name'] و $b['page_name'] للترتيب
    $valA = $a['page_name'] ?? '';
    $valB = $b['page_name'] ?? '';

    // للترتيب حسب الحالة (الأخطاء أولاً، ثم التحذيرات، ثم النجاح)
    if ($sort_by == 'status') {
        $status_order = ['error' => 1, 'warning' => 2, 'no_new_comments' => 3, 'success' => 4, 'never_run' => 5, '' => 6]; // ترتيب الأولوية
        $statusA_val = '';
        $statusB_val = '';

        // تحديد حالة الصفحة بناءً على حساباتها
        $has_error_A = false; $has_expiry_warning_A = false; $active_accounts_A = 0;
        foreach ($a['accounts'] as $acc) {
            if ($acc['last_run_status'] == 'error' || (isset($acc['token_expiry_date']) && strtotime($acc['token_expiry_date']) < time())) $has_error_A = true;
            if (isset($acc['token_expiry_date']) && strtotime($acc['token_expiry_date']) < strtotime('+14 days') && strtotime($acc['token_expiry_date']) > time()) $has_expiry_warning_A = true;
            if ($acc['is_active']) $active_accounts_A++;
        }
        if ($has_error_A) $statusA_val = 'error';
        elseif ($has_expiry_warning_A) $statusA_val = 'warning';
        elseif ($active_accounts_A > 0) $statusA_val = 'success';
        else $statusA_val = 'info'; // 'info' for inactive but no error/warning

        $has_error_B = false; $has_expiry_warning_B = false; $active_accounts_B = 0;
        foreach ($b['accounts'] as $acc) {
            if ($acc['last_run_status'] == 'error' || (isset($acc['token_expiry_date']) && strtotime($acc['token_expiry_date']) < time())) $has_error_B = true;
            if (isset($acc['token_expiry_date']) && strtotime($acc['token_expiry_date']) < strtotime('+14 days') && strtotime($acc['token_expiry_date']) > time()) $has_expiry_warning_B = true;
            if ($acc['is_active']) $active_accounts_B++;
        }
        if ($has_error_B) $statusB_val = 'error';
        elseif ($has_expiry_warning_B) $statusB_val = 'warning';
        elseif ($active_accounts_B > 0) $statusB_val = 'success';
        else $statusB_val = 'info'; // 'info' for inactive but no error/warning

        $orderA = $status_order[$statusA_val] ?? 99;
        $orderB = $status_order[$statusB_val] ?? 99;

        if ($sort_order == 'ASC') {
            return $orderA <=> $orderB;
        } else {
            return $orderB <=> $orderA;
        }
    }
    // الترتيب الافتراضي حسب اسم الصفحة
    else {
        if ($sort_order == 'ASC') {
            return strcasecmp($a['page_name'] ?? '', $b['page_name'] ?? '');
        } else {
            return strcasecmp($b['page_name'] ?? '', $a['page_name'] ?? '');
        }
    }
});

// ترتيب الحسابات داخل كل صفحة (دائماً حسب الاسم للحفاظ على التنظيم داخل التفاصيل)
foreach ($pages_structured as &$page) {
    usort($page['accounts'], function($a, $b) {
        return strcasecmp($a['account_name'], $b['account_name']);
    });
}
unset($page); // Break the reference
// **********************

?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>إدارة صفحات الفيسبوك والحسابات</title>
    <link rel="stylesheet" href="style.css">
    <style>
        .page-summary-card {
            border: 1px solid #ddd;
            padding: 15px;
            margin-bottom: 15px;
            border-radius: 8px;
            background-color: #fff;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .page-summary-card h3 {
            margin-top: 0;
            margin-bottom: 10px;
        }
        .page-summary-card h3 a {
            color: #007bff;
            text-decoration: none;
        }
        .page-summary-card h3 a:hover {
            text-decoration: underline;
        }
        .status-indicator {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 5px;
            font-size: 0.8em;
            font-weight: bold;
            margin-right: 5px;
        }
        .status-ok { background-color: #d4edda; color: #155724; } /* أخضر فاتح */
        .status-warning { background-color: #fff3cd; color: #856404; } /* أصفر فاتح */
        .status-error { background-color: #f8d7da; color: #721c24; } /* أحمر فاتح */
        .status-info { background-color: #d1ecf1; color: #0c5460; } /* أزرق فاتح */

        .search-form, .sort-options {
            margin-bottom: 20px;
            display: flex;
            gap: 10px;
            align-items: center;
        }
        .search-form input[type="text"] {
            flex-grow: 1;
        }
        .sort-options label {
            white-space: nowrap;
        }
        .sort-options select, .sort-options button {
            padding: 8px;
            border-radius: 4px;
            border: 1px solid #ccc;
        }
    </style>
</head>
<body>
    <?php include 'navbar.php'; ?>
    <div class="wrapper">
        <h2>إدارة صفحات الفيسبوك والحسابات</h2>
        <a href="add_page.php" class="btn btn-primary">إضافة صفحة/حساب جديد</a>
        <a href="settings.php" class="btn btn-info">الإعدادات الافتراضية</a>

        <form method="get" action="pages.php" class="search-form">
            <input type="text" name="search" placeholder="ابحث عن صفحة أو حساب..." value="<?php echo htmlspecialchars($search_query); ?>" class="form-control">
            <button type="submit" class="btn btn-primary">بحث</button>
            <?php if (!empty($search_query)): ?>
                <a href="pages.php" class="btn btn-secondary">إلغاء البحث</a>
            <?php endif; ?>
        </form>

        <form method="get" action="pages.php" class="sort-options">
            <label for="sort_by">الترتيب حسب:</label>
            <select name="sort_by" id="sort_by">
                <option value="page_name" <?php echo ($sort_by == 'page_name') ? 'selected' : ''; ?>>اسم الصفحة</option>
                <option value="status" <?php echo ($sort_by == 'status') ? 'selected' : ''; ?>>الحالة</option>
            </select>
            <select name="sort_order" id="sort_order">
                <option value="ASC" <?php echo ($sort_order == 'ASC') ? 'selected' : ''; ?>>تصاعدي</option>
                <option value="DESC" <?php echo ($sort_order == 'DESC') ? 'selected' : ''; ?>>تنازلي</option>
            </select>
            <input type="hidden" name="search" value="<?php echo htmlspecialchars($search_query); ?>">
            <button type="submit" class="btn btn-secondary">ترتيب</button>
        </form>

        <?php if (empty($pages_structured)): ?>
            <p>لم تقم بإضافة أي صفحات أو حسابات بعد.</p>
        <?php else: ?>
            <?php foreach ($pages_structured as $page_db_id => $page_data): ?>
                <div class="page-summary-card">
                    <h3>
                        <a href="page_details.php?page_id=<?php echo htmlspecialchars($page_data['page_db_id_for_display']); ?>">
                            <?php echo htmlspecialchars($page_data['page_name']); ?> (ID: <?php echo htmlspecialchars($page_data['page_db_id_for_display']); ?>)
                        </a>
                        <?php 
                        $has_error = false;
                        $has_expiry_warning = false;
                        $active_accounts_count = 0;
                        $inactive_accounts_count = 0;

                        // **** تحديد اسم الحساب النشط حاليا للصفحة ****
                        $active_account_name_for_page = 'غير محدد';
                        if (!empty($page_data['current_active_account_id'])) {
                            foreach($page_data['accounts'] as $acc) {
                                if ($acc['account_db_id'] == $page_data['current_active_account_id']) {
                                    $active_account_name_for_page = $acc['account_name'];
                                    break;
                                }
                            }
                        }
                        // ********************************************

                        foreach ($page_data['accounts'] as $account) {
                            if ($account['is_active']) {
                                $active_accounts_count++;
                            } else {
                                $inactive_accounts_count++;
                            }
                            if ($account['last_run_status'] == 'error') {
                                $has_error = true;
                            }
                            // التحقق من انتهاء صلاحية التوكن (قبل أسبوعين)
                            if (!empty($account['token_expiry_date'])) {
                                // تحويل تاريخ انتهاء التوكن من UTC إلى Unix timestamp للمقارنة
                                $expiry_datetime_utc = new DateTime($account['token_expiry_date'], new DateTimeZone('UTC'));
                                $expiry_timestamp = $expiry_datetime_utc->getTimestamp();

                                if ($expiry_timestamp < strtotime('+14 days') && $expiry_timestamp > time()) {
                                    $has_expiry_warning = true;
                                } elseif ($expiry_timestamp < time()) {
                                    $has_error = true; // يعتبر خطأ إذا انتهت صلاحيته بالفعل
                                }
                            }
                        }

                        if ($has_error) {
                            echo '<span class="status-indicator status-error">مشكلة</span>';
                        } elseif ($has_expiry_warning) {
                            echo '<span class="status-indicator status-warning">توكن سينتهي</span>';
                        } elseif ($active_accounts_count > 0) {
                            echo '<span class="status-indicator status-ok">مفعلة</span>';
                        } else {
                            echo '<span class="status-indicator status-info">معطلة</span>';
                        }
                        ?>
                    </h3>
                    <p>الحسابات المفعلة: <?php echo $active_accounts_count; ?></p>
                    <p>الحسابات المعطلة: <?php echo $inactive_accounts_count; ?></p>
                    <p>
                        <strong>الحساب النشط حالياً:</strong> 
                        <?php echo htmlspecialchars($active_account_name_for_page); ?>
                        <?php if (!empty($page_data['last_rotation_start_time'])): 
                            // تحويل وقت بداية الدورة من UTC إلى توقيت القاهرة للعرض
                            $rotation_start_dt_utc = new DateTime($page_data['last_rotation_start_time'], new DateTimeZone('UTC'));
                            $rotation_start_dt_cairo = clone $rotation_start_dt_utc;
                            $rotation_start_dt_cairo->setTimezone(new DateTimeZone('Africa/Cairo'));
                        ?>
                            (منذ: <?php echo time_ago_in_arabic($rotation_start_dt_cairo->format('Y-m-d H:i:s')); ?>)
                        <?php endif; ?>
                    </p>
                </div>
            <?php endforeach; ?>
        <?php endif; ?>
    </div>
</body>
</html>
