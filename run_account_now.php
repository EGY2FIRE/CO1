<?php
// run_account_now.php
// هذا السكريبت مخصص للتشغيل اليدوي/الاختبار لحساب واحد فقط.

// 1. تضمين ملف الإعدادات وقاعدة البيانات
require_once __DIR__ . '/config.php';
require_once __DIR__ . '/path/to/facebook-php-sdk-v5/src/Facebook/autoload.php'; // Facebook SDK
require_once __DIR__ . '/helpers.php'; // الدوال المساعدة

// دالة تسجيل الأنشطة (نفس الدالة المستخدمة في الكرون جوب)
function log_activity($message, $level = 'info') {
    $log_directory = __DIR__ . '/logs/';
    // تحقق من وجود المجلد، إذا لم يكن موجودًا، قم بإنشائه
    if (!is_dir($log_directory)) {
        if (!mkdir($log_directory, 0755, true)) {
            error_log("Failed to create log directory: " . $log_directory);
            error_log("[$level] $message"); // سجل الخطأ في سجل PHP الافتراضي
            return;
        }
    }
    $log_file = $log_directory . 'manual_run_activity.log'; // سجل خاص بالتشغيل اليدوي
    $timestamp = date('Y-m-d H:i:s');
    file_put_contents($log_file, "[$timestamp][$level] $message\n", FILE_APPEND);
}

// التحقق من أن الطلب جاء من مستخدم مسجل الدخول
// بما أن هذا السكريبت سيتم استدعاؤه مباشرة من المتصفح، يجب أن نضمن بدء الجلسة هنا
if (session_status() == PHP_SESSION_NONE) {
    session_start();
}
if (!isset($_SESSION['user_id'])) {
    log_activity("Unauthorized access attempt to run_account_now.php.", "warning");
    die("Unauthorized access. Please log in.");
}

// التحقق من وجود معرف الحساب في طلب GET
if (!isset($_GET['account_id']) || !is_numeric($_GET['account_id'])) {
    log_activity("Invalid or missing account_id for manual run.", "error");
    die("Invalid account ID provided.");
}

$account_db_id = (int)$_GET['account_id']; // تحويل إلى عدد صحيح
$current_user_id = $_SESSION['user_id'];

// تهيئة متغيرات حالة التشغيل للحساب (سيتم تحديثها في النهاية)
$runStatusAccount = 'error'; // الافتراضي هو خطأ حتى يثبت العكس
$runMessageAccount = 'لم يتم التشغيل بعد.';
$errorMessageAccount = null;
$commentsRepliedInRun = 0;
$commentsReactedInRun = 0;
$currentLatestCommentTime = null; // لتتبع أحدث وقت تعليق تم الرد عليه

// جلب تفاصيل الحساب المحدد
try {
    $stmtAccount = $pdo->prepare("SELECT fpa.*, fp.page_name, fp.page_id_fb, fp.user_id AS page_owner_user_id
                                   FROM facebook_page_accounts fpa
                                   JOIN facebook_pages fp ON fpa.page_id = fp.id
                                   WHERE fpa.id = :account_id AND fpa.user_id = :user_id");
    $stmtAccount->execute([':account_id' => $account_db_id, ':user_id' => $current_user_id]);
    $pageAccount = $stmtAccount->fetch();

    if (!$pageAccount) {
        log_activity("Account ID {$account_db_id} not found or not owned by user {$current_user_id}.", "error");
        die("Account not found or not authorized for this user.");
    }

    // تهيئة Facebook SDK
    $fb = new Facebook([
        'app_id' => FB_APP_ID,
        'app_secret' => FB_APP_SECRET,
        'default_graph_version' => FB_GRAPH_VERSION,
        'http_client_handler' => 'curl',
        'curl_opts' => [
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => 0,
        ],
    ]);

    // استخراج البيانات من $pageAccount
    $dbPageId = $pageAccount['page_id'];
    $pageName = $pageAccount['page_name'];
    $pageIdFb = $pageAccount['page_id_fb'];
    $accountName = $pageAccount['account_name'];
    $pageAccessToken = $pageAccount['access_token'];
    $lastProcessedTime = $pageAccount['last_processed_comment_time']; // القيمة الأصلية من قاعدة البيانات
    $replyIntervalSeconds = $pageAccount['reply_interval_seconds'] ?? 30;
    $commentsLimit = $pageAccount['comments_limit_per_run'] ?? 10;
    $pageOwnerUserId = $pageAccount['page_owner_user_id'];
    $customSinceHours = $pageAccount['custom_since_hours'];
    $enableReactions = $pageAccount['enable_reactions'];
    $reactionType = $pageAccount['reaction_type'];
    $maxRepliesPerHour = $pageAccount['max_replies_per_hour'];


    log_activity("Manual run started for account: " . $accountName . " on page: " . $pageName, "info");

    // تحديد نطاق زمني ثابت لجلب التعليقات بناءً على custom_since_hours
    $fetchSinceHours = 24; 
    if ($customSinceHours !== null && is_numeric($customSinceHours) && $customSinceHours >= 0) {
        $fetchSinceHours = $customSinceHours;
    }
    $fetchSinceTimestamp = ($fetchSinceHours === 0) ? 0 : strtotime('-' . $fetchSinceHours . ' hours'); 
    log_activity("Fetching comments since: " . date('Y-m-d H:i:s', $fetchSinceTimestamp) . " (last " . ($fetchSinceHours === 0 ? 'all time' : $fetchSinceHours . ' hours') . ") for account '{$accountName}' on page '{$pageName}'.", "debug");
    
    // 8. التحقق من حد الردود في الساعة (`max_replies_per_hour`) للحساب المحدد
    $repliesLastHour = 0;
    try {
        $start_time_utc_for_hourly_stats = (new DateTime('now', new DateTimeZone('UTC')))->modify("-1 hour")->format('Y-m-d H:i:s');
        $stmtHourlyCount = $pdo->prepare("SELECT SUM(reply_count) FROM hourly_reply_stats WHERE fb_page_account_id = :account_id AND hour_timestamp >= :start_time_utc");
        $stmtHourlyCount->execute([':account_id' => $account_db_id, ':start_time_utc' => $start_time_utc_for_hourly_stats]);
        $repliesLastHour = $stmtHourlyCount->fetchColumn() ?? 0;
    } catch (PDOException $e) {
        log_activity("Database error fetching hourly stats for account {$accountName}: " . $e->getMessage(), "error");
        $runStatusAccount = 'error';
        $runMessageAccount = 'خطأ في جلب إحصائيات الردود.';
        $errorMessageAccount = "خطأ قاعدة بيانات عند جلب إحصائيات الردود للحساب: " . $e->getMessage();
        goto end_manual_run; // الانتقال إلى تحديث الحالة النهائية
    }

    if ($repliesLastHour >= $maxRepliesPerHour) {
        log_activity("Account '{$accountName}' (ID: {$account_db_id}) reached hourly reply limit ({$repliesLastHour} replies). Skipping reply processing for this manual run.", "warning");
        $runStatusAccount = 'limit_reached';
        $runMessageAccount = "الحساب '{$accountName}' وصل للحد الأقصى للردود في الساعة ({$repliesLastHour} رد).";
        goto end_manual_run; // الانتقال إلى تحديث الحالة النهائية
    }

    // جلب قوالب الردود (نفس منطق الأولوية)
    $templates_contents = [];
    try {
        $stmtPageTemplates = $pdo->prepare("SELECT rtc.content FROM reply_templates_contents rtc JOIN reply_templates rt ON rtc.template_id = rt.id WHERE rt.page_id = :page_id AND rt.is_active = TRUE");
        $stmtPageTemplates->execute([':page_id' => $dbPageId]);
        $templates_contents = $stmtPageTemplates->fetchAll(PDO::FETCH_COLUMN);

        if (empty($templates_contents)) {
            $stmtDefaultGlobalTemplate = $pdo->prepare("SELECT grtc.content FROM global_reply_template_contents grtc JOIN global_reply_templates grt ON grtc.template_id = grt.id WHERE grt.user_id = :user_id AND grt.is_active = TRUE AND grt.is_default = TRUE");
            $stmtDefaultGlobalTemplate->execute([':user_id' => $pageOwnerUserId]);
            $templates_contents = $stmtDefaultGlobalTemplate->fetchAll(PDO::FETCH_COLUMN);

            if (empty($templates_contents)) {
                $stmtAnyGlobalTemplate = $pdo->prepare("SELECT grtc.content FROM global_reply_template_contents grtc JOIN global_reply_templates grt ON grtc.template_id = grt.id WHERE grt.user_id = :user_id AND grt.is_active = TRUE");
                $stmtAnyGlobalTemplate->execute([':user_id' => $pageOwnerUserId]);
                $templates_contents = $stmtAnyGlobalTemplate->fetchAll(PDO::FETCH_COLUMN);

                if (empty($templates_contents)) {
                    log_activity("No active templates (page-specific or global) found for page '{$pageName}' (account: {$accountName}). Cannot reply.", "warning");
                    $runStatusAccount = 'error';
                    $runMessageAccount = 'لا توجد قوالب ردود نشطة.';
                    $errorMessageAccount = 'لم يتم العثور على قوالب ردود نشطة مخصصة للصفحة ولا قوالب عامة.';
                    goto end_manual_run; // الانتقال إلى تحديث الحالة النهائية
                }
            }
        }
    } catch (PDOException $e) {
        $runStatusAccount = 'error';
        $runMessageAccount = 'خطأ في جلب القوالب.';
        $errorMessageAccount = "خطأ قاعدة بيانات عند جلب القوالب: " . $e->getMessage();
        log_activity($errorMessageAccount, "error");
        goto end_manual_run; // الانتقال إلى تحديث الحالة النهائية
    }

    // تهيئة currentLatestCommentTime بالقيمة الأصلية من قاعدة البيانات
    $currentLatestCommentTime = $pageAccount['last_processed_comment_time']; 

    try {
        $postsResponse = $fb->get(
            "/{$pageIdFb}/posts?fields=id,created_time&limit=5",
            $pageAccessToken
        );
        $postsEdge = $postsResponse->getGraphEdge();

        foreach ($postsEdge as $post) {
            $postId = $post->getField('id');
            log_activity("Fetching comments for post ID: " . $postId . " on page " . $pageName . " via account " . $accountName, "debug");

            $commentsResponse = $fb->get(
                "/{$postId}/comments?fields=from,message,created_time,parent&limit=50&since=" . $fetchSinceTimestamp,
                $pageAccessToken
            );
            $commentsEdge = $commentsResponse->getGraphEdge();

            if ($commentsEdge->count() > 0) {
                log_activity("Found " . $commentsEdge->count() . " comments for post " . $postId . ". Details:", "debug");
                foreach ($commentsEdge as $comment_debug) {
                    $debug_time_utc = $comment_debug->getField('created_time');
                    $debug_time_utc->setTimezone(new DateTimeZone('UTC'));
                    log_activity("  - Comment ID: " . $comment_debug->getField('id') . 
                                 ", From: " . $comment_debug->getField('from')->getField('name') . 
                                 ", Time (UTC): " . $debug_time_utc->format('Y-m-d H:i:s') . 
                                 ", Parent ID: " . ($comment_debug->getField('parent') ? $comment_debug->getField('parent')->getField('id') : 'N/A'), "debug");
                }
            } else {
                log_activity("No new comments found for post " . $postId . " since " . date('Y-m-d H:i:s', $fetchSinceTimestamp) . ".", "debug");
            }

            foreach ($commentsEdge as $comment) {
                $commentId = $comment->getField('id');
                $commentMessage = $comment->getField('message');
                $commentCreator = $comment->getField('from');
                
                $commentDateTime = $comment->getField('created_time');
                $commentDateTime->setTimezone(new DateTimeZone('UTC'));
                $commentCreatedTime = $commentDateTime->format('Y-m-d H:i:s');

                $parentId = $comment->getField('parent') ? $comment->getField('parent')->getField('id') : null;

                if ($parentId) {
                    log_activity("Skipping reply to comment ID {$commentId} as it's a reply to another comment (Parent ID: {$parentId}).", "debug");
                    continue;
                }

                $stmtCheck = $pdo->prepare("SELECT id, fb_page_account_id FROM processed_comments WHERE comment_id_fb = :comment_id AND page_id = :page_id");
                $stmtCheck->execute([':comment_id' => $commentId, ':page_id' => $dbPageId]);

                $alreadyProcessed = ($stmtCheck->rowCount() > 0);

                if (!$alreadyProcessed) {
                    if (empty($templates_contents)) { 
                        log_activity("No templates available for page ID {$dbPageId} (page {$pageName}) after all checks. Cannot reply to {$commentId}.", "error");
                        $runStatusAccount = 'error'; 
                        $runMessageAccount = 'لا توجد قوالب ردود نشطة للرد.';
                        $errorMessageAccount = "لا توجد قوالب ردود نشطة للرد على التعليق {$commentId}.";
                        goto end_manual_run; 
                    }
                    $randomTemplateContent = $templates_contents[array_rand($templates_contents)];
                    $replyMessage = str_replace('{user_name}', htmlspecialchars($commentCreator->getField('name')), $randomTemplateContent);

                    log_activity("Attempting to reply to comment ID {$commentId} from '{$commentCreator->getField('name')}' on page '{$pageName}' via account '{$accountName}'.", "info");

                    try {
                        $response = $fb->post(
                            "/{$commentId}/comments",
                            ['message' => $replyMessage],
                            $pageAccessToken
                        );
                        $graphNode = $response->getGraphNode();
                        
                        $stmtInsert = $pdo->prepare("INSERT IGNORE INTO processed_comments (comment_id_fb, page_id, fb_page_account_id) VALUES (:comment_id, :page_id, :fb_page_account_id)");
                        $stmtInsert->execute([
                            ':comment_id' => $commentId,
                            ':page_id' => $dbPageId,
                            ':fb_page_account_id' => $account_db_id
                        ]);

                        log_activity("Successfully replied to comment ID {$commentId} on page '{$pageName}' via account '{$accountName}'. Reply ID: " . $graphNode->getField('id'), "success");

                        $commentsRepliedInRun++; 
                        if (strtotime($commentCreatedTime) > strtotime($currentLatestCommentTime)) {
                            $currentLatestCommentTime = $commentCreatedTime;
                        }

                        sleep($replyIntervalSeconds);

                    } catch (FacebookResponseException $e) {
                        $runStatusAccount = 'error';
                        $runMessageAccount = 'خطأ في الرد على تعليق.';
                        $errorMessageAccount = "خطأ Graph API عند الرد على تعليق {$commentId}: " . $e->getMessage();
                        log_activity($errorMessageAccount, "error");
                        goto end_manual_run;
                    } catch (FacebookSDKException $e) {
                        $runStatusAccount = 'error';
                        $runMessageAccount = 'خطأ في Facebook SDK.';
                        $errorMessageAccount = "خطأ Facebook SDK عند الرد على تعليق {$commentId}: " . $e->getMessage();
                        log_activity($errorMessageAccount, "error");
                        goto end_manual_run;
                    }

                    if ($commentsRepliedInRun >= $commentsLimit) {
                        log_activity("Reached comments limit ({$commentsLimit}) for account '{$accountName}' on page '{$pageName}'. Stopping further processing for this account.", "info");
                        $runMessageAccount = "تم الرد على {$commentsRepliedInRun} تعليقات.";
                        break 2; // يكسر حلقتي foreach (التعليقات والمنشورات)
                    }
                } else {
                    $replied_by_account_id = $stmtCheck->fetchColumn(1);
                    log_activity("Comment ID {$commentId} on page '{$pageName}' already processed by account ID {$replied_by_account_id}. Skipping.", "debug");
                }

                // منطق الإعجاب بالتعليق
                if ($enableReactions && !$alreadyProcessed) {
                    try {
                        $reactionEndpoint = "/{$commentId}/reactions";
                        $reactionData = ['type' => strtoupper($reactionType)];
                        
                        log_activity("Attempting to react with '{$reactionType}' to comment ID {$commentId} on page '{$pageName}' via account '{$accountName}'.", "info");
                        
                        $fb->post($reactionEndpoint, $reactionData, $pageAccessToken);
                        log_activity("Successfully reacted with '{$reactionType}' to comment ID {$commentId}.", "success");
                    } catch (FacebookResponseException $e) {
                        log_activity("Graph API error when reacting to comment ID {$commentId}: " . $e->getMessage(), "error");
                    } catch (FacebookSDKException $e) {
                        log_activity("Facebook SDK error when reacting to comment ID {$commentId}: " . $e->getMessage(), "error");
                    }
                }

            } // نهاية foreach التعليقات
            
            if ($commentsRepliedInRun >= $commentsLimit) {
                $runMessageAccount = "تم الرد على {$commentsRepliedInRun} تعليقات.";
                break; // يكسر حلقة المنشورات
            }
        } // نهاية foreach المنشورات

        // تحديث إحصائيات الردود الساعية للحساب
        if ($commentsRepliedInRun > 0) {
            $current_hour_utc = (new DateTime('now', new DateTimeZone('UTC')))->format('Y-m-d H:00:00');
            $stmtUpdateHourlyStats = $pdo->prepare("INSERT INTO hourly_reply_stats (fb_page_account_id, hour_timestamp, reply_count) 
                                                    VALUES (:account_id, :hour_ts, :count)
                                                    ON DUPLICATE KEY UPDATE reply_count = reply_count + :count");
            $stmtUpdateHourlyStats->execute([
                ':account_id' => $account_db_id,
                ':hour_ts' => $current_hour_utc,
                ':count' => $commentsRepliedInRun
            ]);
            log_activity("Updated hourly stats for account {$accountName} (Hour UTC: {$current_hour_utc}, Added: {$commentsRepliedInRun} replies).", "info");
        }

        // حساب قيمة last_time_processed_value قبل الاستعلام
        $last_time_processed_value = ($runStatusAccount == 'success' && $commentsRepliedInRun > 0) ? $currentLatestCommentTime : $pageAccount['last_processed_comment_time'];

        // تحديث إحصائيات التشغيل للحساب في قاعدة البيانات
        $stmtUpdateAccountStats = $pdo->prepare("UPDATE facebook_page_accounts SET 
            last_run_status = :status, 
            last_run_message = :message, 
            last_error_message = :error_msg,
            last_processed_comment_time = :last_time_processed
            WHERE id = :db_page_account_id");
        
        $stmtUpdateAccountStats->execute([
            ':status' => $runStatusAccount,
            ':message' => $runMessageAccount,
            ':error_msg' => $errorMessageAccount,
            ':last_time_processed' => $last_time_processed_value, // استخدام القيمة المحسوبة
            ':db_page_account_id' => $account_db_id
        ]);
        log_activity("Updated stats for account '{$accountName}' on page '{$pageName}': Status: {$runStatusAccount}, Message: '{$runMessageAccount}'", "info");
        
    } catch (FacebookResponseException $e) {
        $runStatusAccount = 'error';
        $runMessageAccount = 'خطأ في جلب المنشورات/التعليقات.';
        $errorMessageAccount = "خطأ Graph API عند جلب المنشورات/التعليقات لصفحة '{$pageName}': " . $e->getMessage();
        log_activity($errorMessageAccount, "error");
    } catch (FacebookSDKException $e) {
        $runStatusAccount = 'error';
        $runMessageAccount = 'خطأ في Facebook SDK.';
        $errorMessageAccount = "خطأ Facebook SDK عند جلب المنشورات/التعليقات لصفحة '{$pageName}': " . $e->getMessage();
        log_activity($errorMessageAccount, "error");
    } catch (PDOException $e) {
        $runStatusAccount = 'error';
        $runMessageAccount = 'خطأ قاعدة بيانات أثناء المعالجة.';
        $errorMessageAccount = "خطأ قاعدة بيانات عام للحساب: " . $e->getMessage();
        log_activity($errorMessageAccount, "error");
    }

    end_manual_run:; // نقطة القفز هنا للتشغيل اليدوي

    // لا يوجد تحرير قفل هنا، لأنه تشغيل لحساب واحد ولا يؤثر على القفل العام للصفحة.
    unset($pdo);
    log_activity("Manual run finished for account '{$accountName}'.", "info");
} // نهاية كتلة try الرئيسية
?>
