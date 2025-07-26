<?php
require_once 'auth_check.php';
require_once 'config.php';
require_once 'helpers.php'; // تضمين ملف الدوال المساعدة

if (!isset($_GET['account_id']) || !is_numeric($_GET['account_id'])) {
    header("location: pages.php");
    exit();
}

$account_id = $_GET['account_id'];
$current_user_id = $_SESSION['user_id'];

// التحقق من أن الحساب يتبع المستخدم الحالي
$stmtAccount = $pdo->prepare("SELECT fpa.id, fpa.account_name, fp.page_name, fp.id AS page_db_id
                               FROM facebook_page_accounts fpa
                               JOIN facebook_pages fp ON fpa.page_id = fp.id
                               WHERE fpa.id = :account_id AND fpa.user_id = :user_id");
$stmtAccount->execute([':account_id' => $account_id, ':user_id' => $current_user_id]);
$account = $stmtAccount->fetch();

if (!$account) {
    echo "الحساب غير موجود أو لا تملك صلاحية الوصول إليه.";
    exit();
}

$page_db_id = $account['page_db_id'];

// جلب الإحصائيات الساعية لآخر 24 ساعة (بتوقيت UTC)
$hourly_stats = [];
$now_utc = new DateTime('now', new DateTimeZone('UTC'));
$now_utc->setTime($now_utc->format('H'), 0, 0); // تقريب للوقت الحالي للساعة الكاملة بتوقيت UTC
$end_time_utc = $now_utc->format('Y-m-d H:i:s');

$start_time_dt_utc = clone $now_utc;
$start_time_dt_utc->modify('-23 hours'); // 24 ساعة تشمل الساعة الحالية و 23 ساعة سابقة بتوقيت UTC
$start_time_utc = $start_time_dt_utc->format('Y-m-d H:i:s');


$stmtStats = $pdo->prepare("SELECT hour_timestamp, reply_count 
                            FROM hourly_reply_stats 
                            WHERE fb_page_account_id = :account_id 
                            AND hour_timestamp >= :start_time_utc 
                            AND hour_timestamp <= :end_time_utc
                            ORDER BY hour_timestamp ASC");
$stmtStats->execute([':account_id' => $account_id, ':start_time_utc' => $start_time_utc, ':end_time_utc' => $end_time_utc]);
$raw_stats = $stmtStats->fetchAll();

// تجميع الإحصائيات في مصفوفة لضمان وجود جميع الساعات حتى لو كانت 0
$stats_map = [];
foreach ($raw_stats as $row) {
    $stats_map[$row['hour_timestamp']] = $row['reply_count'];
}

// ملء جميع الساعات في النطاق (24 ساعة)
for ($i = 0; $i <= 23; $i++) {
    $hour_dt_utc = clone $start_time_dt_utc;
    $hour_dt_utc->modify("+$i hours");
    $hour_key_utc = $hour_dt_utc->format('Y-m-d H:00:00');
    $hourly_stats[$hour_key_utc] = $stats_map[$hour_key_utc] ?? 0;
}

?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>إحصائيات <?php echo htmlspecialchars($account['account_name']); ?></title>
    <link rel="stylesheet" href="style.css">
    <style>
        .stats-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        .stats-table th, .stats-table td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: right;
        }
        .stats-table th {
            background-color: #f2f2f2;
        }
        .stats-table tr:nth-child(even) {
            background-color: #f9f9f9;
        }
    </style>
</head>
<body>
    <?php include 'navbar.php'; ?>
    <div class="wrapper">
        <h2>إحصائيات الردود لحساب: <?php echo htmlspecialchars($account['account_name']); ?></h2>
        <p>مربوط بالصفحة: <a href="page_details.php?page_id=<?php echo $page_db_id; ?>"><?php echo htmlspecialchars($account['page_name']); ?></a></p>
        <a href="page_details.php?page_id=<?php echo $page_db_id; ?>" class="btn btn-secondary">العودة إلى تفاصيل الصفحة</a>

        <h3>الردود في آخر 24 ساعة (بالساعة)</h3>
        <?php if (empty($hourly_stats) || array_sum($hourly_stats) == 0): ?>
            <p>لا توجد إحصائيات ردود متاحة لهذا الحساب بعد.</p>
        <?php else: ?>
            <table class="stats-table">
                <thead>
                    <tr>
                        <th>الساعة (توقيت القاهرة)</th>
                        <th>عدد الردود</th>
                    </tr>
                </thead>
                <tbody>
                    <?php 
                    $total_24_hours = 0;
                    // عرض الساعات بترتيب تنازلي (الأحدث أولاً)
                    krsort($hourly_stats); 
                    foreach ($hourly_stats as $hour_ts_utc => $count): 
                        $total_24_hours += $count;
                        // **** تحويل من UTC إلى توقيت القاهرة للعرض ****
                        $display_hour_dt = new DateTime($hour_ts_utc, new DateTimeZone('UTC'));
                        $display_hour_dt->setTimezone(new DateTimeZone('Africa/Cairo'));
                    ?>
                        <tr>
                            <td><?php echo htmlspecialchars($display_hour_dt->format('Y-m-d H:00')); ?> (<?php echo time_ago_in_arabic($display_hour_dt->format('Y-m-d H:i:s')); ?>)</td>
                            <td><?php echo htmlspecialchars($count); ?></td>
                        </tr>
                    <?php endforeach; ?>
                    <tr>
                        <th colspan="1">الإجمالي في آخر 24 ساعة:</th>
                        <th><?php echo $total_24_hours; ?></th>
                    </tr>
                </tbody>
            </table>
        <?php endif; ?>
    </div>
</body>
</html>
