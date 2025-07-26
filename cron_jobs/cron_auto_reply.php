
<?php
// cron_auto_reply.php
// هذا السكريبت مصمم للتشغيل بواسطة Cron Job بشكل متكرر (مثلاً كل 5-10 دقائق)

// 1. تضمين ملف الإعدادات والاتصال بقاعدة البيانات
// نفترض أن config.php موجود في المجلد الأب (المجلد الرئيسي للمشروع)
require_once __DIR__ . '/../config.php';

// 2. تضمين Facebook SDK
// نفترض أن مجلد 'vendor' (الناتج عن Composer) موجود في المجلد الأب (المجلد الرئيسي للمشروع)
require_once __DIR__ . '/../vendor/autoload.php';

// استخدام الـ Classes المطلوبة من Facebook SDK
use Facebook\Facebook;
use Facebook\Exceptions\FacebookResponseException;
use Facebook\Exceptions\FacebookSDKException;

// 3. دالة بسيطة لتسجيل الأنشطة والأخطاء
// سيتم حفظ السجلات في ملف bot_activity.log داخل مجلد logs في جذر المشروع
function log_activity($message, $level = 'info') {
    // تحديد مسار مجلد السجلات (المجلد الأب للمجلد الحالي ثم مجلد logs)
    $log_directory = __DIR__ . '/../logs/';
    
    // إنشاء المجلد إذا لم يكن موجودًا
    if (!is_dir($log_directory)) {
        if (!mkdir($log_directory, 0755, true)) { // 0755 أذونات مناسبة
            error_log("Failed to create log directory: " . $log_directory);
            // إذا فشل إنشاء المجلد، قم بتسجيل الأخطاء في سجل PHP الافتراضي
            error_log("[$level] $message");
            return;
        }
    }
    
    $log_file = $log_directory . 'bot_activity.log';
    $timestamp = date('Y-m-d H:i:s');
    file_put_contents($log_file, "[$timestamp][$level] $message\n", FILE_APPEND);
}

log_activity("Cron job started for auto-reply. " . date('Y-m-d H:i:s'), "info");

// 4. تهيئة كائن Facebook SDK
try {
    $fb = new Facebook([
        'app_id' => FB_APP_ID,
        'app_secret' => FB_APP_SECRET,
        'default_graph_version' => FB_GRAPH_VERSION,
        // **** الحل البديل لمشكلة SSL: تعطيل التحقق من الشهادة ****
        // تحذير: هذا يقلل من أمان اتصالك. لا يُنصح به في بيئات الإنتاج.
        // إذا استمرت المشكلة، تأكد أن هذا الكود يتم تشغيله فعليًا على الخادم
        // (قد تحتاج لإعادة تشغيل PHP-FPM/خادم الويب بعد الرفع).
        'http_client_handler' => 'curl', // تأكد من استخدام cURL
        'curl_opts' => [
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_SSL_VERIFYHOST => 0, // 0 لتعطيل التحقق من اسم المضيف
        ],
        // ******************************************************
    ]);
} catch (Exception $e) {
    log_activity("Failed to initialize Facebook SDK: " . $e->getMessage(), "critical");
    unset($pdo);
    exit(); // إنهاء السكريبت إذا فشلت التهيئة الأساسية
}


// 5. جلب جميع الصفحات التي لديها حسابات مفعلة (مع معلومات الحسابات)
try {
    $stmt = $pdo->prepare("SELECT fp.id AS page_db_id, fp.page_name, fp.page_id_fb, fp.user_id AS page_owner_user_id, 
                                  fp.is_processing, fp.current_active_account_id, fp.last_rotation_start_time,
                                  fpa.id AS account_db_id, fpa.account_name, fpa.access_token, fpa.last_processed_comment_time,
                                  fpa.reply_interval_seconds, fpa.comments_limit_per_run, fpa.custom_since_hours,
                                  fpa.enable_reactions, fpa.reaction_type, fpa.rotation_duration_minutes, fpa.max_replies_per_hour
                           FROM facebook_pages fp
                           JOIN facebook_page_accounts fpa ON fp.id = fpa.page_id
                           WHERE fpa.is_active = TRUE AND fp.user_id = fpa.user_id
                           ORDER BY fp.id, fpa.id"); // ترتيب لضمان تسلسل ثابت للحسابات

    $stmt->execute();
    $rawActivePageAccounts = $stmt->fetchAll();

    // **** تحديث last_cron_run_time للمستخدمين المعنيين بهذا الكرون جوب ****
    $processedUserIds = [];
    foreach ($rawActivePageAccounts as $row) {
        $processedUserIds[$row['page_owner_user_id']] = true;
    }

    if (!empty($processedUserIds)) {
        $userPlaceholders = implode(',', array_fill(0, count($processedUserIds), '?'));
        $stmtUpdateLastCronRun = $pdo->prepare("UPDATE users SET last_cron_run_time = UTC_TIMESTAMP() WHERE id IN ($userPlaceholders)");
        $stmtUpdateLastCronRun->execute(array_keys($processedUserIds));
        log_activity("Updated last_cron_run_time for " . count($processedUserIds) . " user(s).", "info");
    }
    // **********************************************************************

    // تجميع الحسابات حسب الصفحة
    $activePagesWithAccounts = [];
    foreach ($rawActivePageAccounts as $row) {
        $pageId = $row['page_db_id'];
        if (!isset($activePagesWithAccounts[$pageId])) {
            $activePagesWithAccounts[$pageId] = [
                'page_db_id' => $row['page_db_id'],
                'page_name' => $row['page_name'],
                'page_id_fb' => $row['page_id_fb'],
                'page_owner_user_id' => $row['page_owner_user_id'],
                'is_processing' => $row['is_processing'],
                'current_active_account_id' => $row['current_active_account_id'],
                'last_rotation_start_time' => $row['last_rotation_start_time'],
                'accounts' => []
            ];
        }
        $activePagesWithAccounts[$pageId]['accounts'][] = $row; // إضافة تفاصيل الحساب الكاملة
    }

    log_activity("Found " . count($activePagesWithAccounts) . " active pages with accounts to process.", "info");

} catch (PDOException $e) {
    log_activity("Database error when fetching active page-account links: " . $e->getMessage(), "error");
    unset($pdo);
    exit();
}

// 6. تكرار على كل صفحة (وليس كل حساب) للمعالجة، وتطبيق القفل على مستوى الصفحة
foreach ($activePagesWithAccounts as $page) {
    // تأخير عشوائي بسيط قبل محاولة الحصول على القفل للصفحة
    usleep(mt_rand(0, 500000)); // تأخير من 0 إلى 0.5 ثانية لكل محاولة صفحة

    $pageDbId = $page['page_db_id'];
    $pageName = $page['page_name'];
    $pageIdFb = $page['page_id_fb'];
    $pageOwnerUserId = $page['page_owner_user_id'];
    $currentActiveAccountId = $page['current_active_account_id'];
    $lastRotationStartTime = $page['last_rotation_start_time'];
    $accountsForPage = $page['accounts']; // جميع الحسابات المفعلة لهذه الصفحة

    // تهيئة متغيرات حالة التشغيل للصفحة
    $pageRunStatus = 'success';
    $pageRunMessage = 'تمت معالجة الصفحة بنجاح.';
    $pageErrorMessage = null;

    // **** محاولة الحصول على القفل للصفحة ****
    try {
        $stmtAcquireLock = $pdo->prepare("UPDATE facebook_pages SET is_processing = TRUE WHERE id = :page_id AND is_processing = FALSE");
        $stmtAcquireLock->execute([':page_id' => $pageDbId]);
        
        if ($stmtAcquireLock->rowCount() == 0) {
            log_activity("Page '{$pageName}' (ID: {$pageDbId}) is already being processed by another cron job instance. Skipping.", "warning");
            continue; // تخطي هذه الصفحة
        }
        log_activity("Successfully acquired lock for page '{$pageName}' (ID: {$pageDbId}).", "info");

        $selectedAccountToProcess = null;
        $nextActiveAccountId = null; // الحساب الذي سيصبح نشطًا بعد هذه الدورة (إذا تم التبديل)
        $newRotationStartTime = null;

        $nowUtc = new DateTime('now', new DateTimeZone('UTC'));

        // 7. منطق التبديل الزمني للحسابات (Rotation Logic)
        if (empty($accountsForPage)) {
            log_activity("No active accounts found for page '{$pageName}'. Cannot process.", "warning");
            $pageRunStatus = 'error';
            $pageRunMessage = 'لا توجد حسابات نشطة للصفحة.';
            $pageErrorMessage = 'لم يتم العثور على أي حسابات نشطة لربطها بهذه الصفحة.';
            goto end_page_processing; // تخطي معالجة هذه الصفحة
        }

        // 7.1. تحديد الحساب الحالي في الدوران
        $currentAccountInRotation = null;
        if (!empty($currentActiveAccountId)) {
            foreach ($accountsForPage as $acc) {
                if ($acc['account_db_id'] == $currentActiveAccountId) {
                    $currentAccountInRotation = $acc;
                    break;
                }
            }
        }

        $performRotation = false;
        // إذا لم يتم تعيين حساب نشط للصفحة مطلقًا
        if (empty($currentAccountInRotation) || empty($lastRotationStartTime)) {
            $performRotation = true; 
            log_activity("Page '{$pageName}' has no active account or rotation time. Initializing rotation.", "debug");
        } else {
            $rotationStartDateTime = new DateTime($lastRotationStartTime, new DateTimeZone('UTC'));
            $elapsedMinutes = ($nowUtc->getTimestamp() - $rotationStartDateTime->getTimestamp()) / 60;

            // إذا انتهت مدة الحساب الحالي
            if ($elapsedMinutes >= $currentAccountInRotation['rotation_duration_minutes']) {
                $performRotation = true; 
                log_activity("Account '{$currentAccountInRotation['account_name']}' rotation duration ({$currentAccountInRotation['rotation_duration_minutes']} mins) ended for page '{$pageName}'. Performing rotation.", "debug");
            } else {
                // الحساب الحالي لم تنتهِ مدته بعد، حاول استخدامه
                $selectedAccountToProcess = $currentAccountInRotation;
                log_activity("Account '{$currentAccountInRotation['account_name']}' is still within its rotation duration for page '{$pageName}'.", "debug");
            }
        }

        if ($performRotation) {
            // تحديد الحساب التالي في الدوران
            $accountIds = array_column($accountsForPage, 'account_db_id'); // IDs فقط
            $nextIndex = 0;
            if (!empty($currentAccountInRotation) && in_array($currentAccountInRotation['account_db_id'], $accountIds)) {
                $currentIndex = array_search($currentAccountInRotation['account_db_id'], $accountIds);
                $nextIndex = ($currentIndex + 1) % count($accountIds);
            }
            $nextActiveAccountId = $accountIds[$nextIndex];

            // جلب تفاصيل الحساب الجديد
            foreach ($accountsForPage as $acc) {
                if ($acc['account_db_id'] == $nextActiveAccountId) {
                    $selectedAccountToProcess = $acc;
                    break;
                }
            }
            $newRotationStartTime = $nowUtc->format('Y-m-d H:i:s'); // وقت بداية دورة الحساب الجديد
            log_activity("Rotated page '{$pageName}' to account '{$selectedAccountToProcess['account_name']}' (ID: {$selectedAccountToProcess['account_db_id']}). New rotation start time: {$newRotationStartTime}", "info");
        }

        if (empty($selectedAccountToProcess)) {
            log_activity("No suitable account found or selected for page '{$pageName}' after rotation logic. Skipping page processing.", "warning");
            $pageRunStatus = 'no_active_account';
            $pageRunMessage = 'لا يوجد حساب نشط مناسب حالياً للصفحة.';
            goto end_page_processing;
        }

        // 8. التحقق من حد الردود في الساعة (`max_replies_per_hour`) للحساب المحدد
        $repliesLastHour = 0;
        try {
            $start_time_utc_for_hourly_stats = (new DateTime('now', new DateTimeZone('UTC')))->modify("-1 hour")->format('Y-m-d H:i:s');
            $stmtHourlyCount = $pdo->prepare("SELECT SUM(reply_count) FROM hourly_reply_stats WHERE fb_page_account_id = :account_id AND hour_timestamp >= :start_time_utc");
            $stmtHourlyCount->execute([':account_id' => $selectedAccountToProcess['account_db_id'], ':start_time_utc' => $start_time_utc_for_hourly_stats]);
            $repliesLastHour = $stmtHourlyCount->fetchColumn() ?? 0;
        } catch (PDOException $e) {
            log_activity("Database error fetching hourly stats for account {$selectedAccountToProcess['account_name']}: " . $e->getMessage(), "error");
            $pageRunStatus = 'error';
            $pageRunMessage = 'خطأ في جلب إحصائيات الردود.';
            $pageErrorMessage = "خطأ قاعدة بيانات عند جلب إحصائيات الردود للحساب: " . $e->getMessage();
            goto end_page_processing;
        }

        if ($repliesLastHour >= $selectedAccountToProcess['max_replies_per_hour']) {
            log_activity("Account '{$selectedAccountToProcess['account_name']}' (ID: {$selectedAccountToProcess['account_db_id']}) reached hourly reply limit ({$repliesLastHour} replies). Skipping reply processing for this account in this run.", "warning");
            $pageRunStatus = 'limit_reached';
            $pageRunMessage = "الحساب '{$selectedAccountToProcess['account_name']}' وصل للحد الأقصى للردود في الساعة ({$repliesLastHour} رد).";
            // لا نغير current_active_account_id أو last_rotation_start_time هنا
            // لأنه ما زال دوره، لكنه متوقف بسبب الحد.
            goto end_page_processing;
        }

        // **** الآن نقوم بمعالجة التعليقات باستخدام الحساب المحدد ($selectedAccountToProcess) ****
        $dbPageAccountId = $selectedAccountToProcess['account_db_id'];
        $accountName = $selectedAccountToProcess['account_name'];
        $pageAccessToken = $selectedAccountToProcess['access_token'];
        $lastProcessedTime = $selectedAccountToProcess['last_processed_comment_time'];
        $replyIntervalSeconds = $selectedAccountToProcess['reply_interval_seconds'] ?? 30;
        $commentsLimit = $selectedAccountToProcess['comments_limit_per_run'] ?? 10;
        $customSinceHours = $selectedAccountToProcess['custom_since_hours'];
        $enableReactions = $selectedAccountToProcess['enable_reactions'];
        $reactionType = $selectedAccountToProcess['reaction_type'];

        log_activity("Processing comments for account: " . $accountName . " on page: " . $pageName . ".", "info");

        $fetchSinceHours = 24; 
        if ($customSinceHours !== null && is_numeric($customSinceHours) && $customSinceHours >= 0) {
            $fetchSinceHours = $customSinceHours;
        }
        $fetchSinceTimestamp = ($fetchSinceHours === 0) ? 0 : strtotime('-' . $fetchSinceHours . ' hours'); 
        log_activity("Fetching comments since: " . date('Y-m-d H:i:s', $fetchSinceTimestamp) . " (last " . ($fetchSinceHours === 0 ? 'all time' : $fetchSinceHours . ' hours') . ") for account '{$accountName}' on page '{$pageName}'.", "debug");
        
        $runStatusAccount = 'success'; // حالة هذا الحساب في هذه الجولة
        $runMessageAccount = 'تمت المعالجة بنجاح.';
        $errorMessageAccount = null;
        $commentsRepliedInRun = 0;
        $commentsReactedInRun = 0;

        $templates_contents = [];
        try {
            $stmtPageTemplates = $pdo->prepare("SELECT rtc.content FROM reply_templates_contents rtc JOIN reply_templates rt ON rtc.template_id = rt.id WHERE rt.page_id = :page_id AND rt.is_active = TRUE");
            $stmtPageTemplates->execute([':page_id' => $pageDbId]);
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
                        goto end_account_processing_block; // تخطي كتلة معالجة التعليقات
                    }
                }
            }
        } catch (PDOException $e) {
            $runStatusAccount = 'error';
            $runMessageAccount = 'خطأ في جلب القوالب.';
            $errorMessageAccount = "خطأ قاعدة بيانات عند جلب القوالب: " . $e->getMessage();
            log_activity($errorMessageAccount, "error");
            goto end_account_processing_block;
        }

        $currentLatestCommentTime = $selectedAccountToProcess['last_processed_comment_time']; // متغير لتتبع أحدث وقت تعليق تم الرد عليه في هذه الجولة


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
                    $stmtCheck->execute([':comment_id' => $commentId, ':page_id' => $pageDbId]);

                    $alreadyProcessed = ($stmtCheck->rowCount() > 0);

                    if (!$alreadyProcessed) {
                        if (empty($templates_contents)) { 
                            log_activity("No templates available for page ID {$pageDbId} (page {$pageName}) after all checks. Cannot reply to {$commentId}.", "error");
                            $runStatusAccount = 'error'; 
                            $runMessageAccount = 'لا توجد قوالب ردود نشطة للرد.';
                            $errorMessageAccount = "لا توجد قوالب ردود نشطة للرد على التعليق {$commentId}.";
                            goto end_account_processing_block; 
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
                                ':page_id' => $pageDbId,
                                ':fb_page_account_id' => $dbPageAccountId
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
                            goto end_account_processing_block;
                        } catch (FacebookSDKException $e) {
                            $runStatusAccount = 'error';
                            $runMessageAccount = 'خطأ في Facebook SDK.';
                            $errorMessageAccount = "خطأ Facebook SDK عند الرد على تعليق {$commentId}: " . $e->getMessage();
                            log_activity($errorMessageAccount, "error");
                            goto end_account_processing_block;
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

                    // **** منطق الإعجاب بالتعليق ****
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
                    // ******************************

                } // نهاية foreach التعليقات
                
                if ($commentsRepliedInRun >= $commentsLimit) {
                    $runMessageAccount = "تم الرد على {$commentsRepliedInRun} تعليقات.";
                    break; // يكسر حلقة المنشورات
                }
            } // نهاية foreach المنشورات

            end_account_processing_block:; // نقطة القفز هنا لكتلة معالجة التعليقات

            // **** تحديث إحصائيات الردود الساعية للحساب ****
            if ($commentsRepliedInRun > 0) {
                $current_hour_utc = (new DateTime('now', new DateTimeZone('UTC')))->format('Y-m-d H:00:00');
                $stmtUpdateHourlyStats = $pdo->prepare("INSERT INTO hourly_reply_stats (fb_page_account_id, hour_timestamp, reply_count) 
                                                        VALUES (:account_id, :hour_ts, :count)
                                                        ON DUPLICATE KEY UPDATE reply_count = reply_count + :count");
                $stmtUpdateHourlyStats->execute([
                    ':account_id' => $dbPageAccountId,
                    ':hour_ts' => $current_hour_utc,
                    ':count' => $commentsRepliedInRun
                ]);
                log_activity("Updated hourly stats for account {$accountName} (Hour UTC: {$current_hour_utc}, Added: {$commentsRepliedInRun} replies).", "info");
            }

            // **** تحديث إحصائيات التشغيل للحساب في قاعدة البيانات ****
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
                ':last_time_processed' => ($runStatusAccount == 'success' && $commentsRepliedInRun > 0) ? $currentLatestCommentTime : $selectedAccountToProcess['last_processed_comment_time'], 
                ':db_page_account_id' => $dbPageAccountId
            ]);
            log_activity("Updated stats for account '{$accountName}' on page '{$pageName}': Status: {$runStatusAccount}, Message: '{$runMessageAccount}'", "info");
            
        } // نهاية حلقة foreach للحسابات داخل الصفحة

    } catch (FacebookResponseException $e) {
        $pageRunStatus = 'error';
        $pageRunMessage = 'خطأ في جلب المنشورات/التعليقات للصفحة.';
        $pageErrorMessage = "خطأ Graph API عام للصفحة '{$pageName}' (ID: {$pageDbId}): " . $e->getMessage();
        log_activity($pageErrorMessage, "error");
    } catch (FacebookSDKException $e) {
        $pageRunStatus = 'error';
        $pageRunMessage = 'خطأ في Facebook SDK للصفحة.';
        $pageErrorMessage = "خطأ Facebook SDK عام للصفحة '{$pageName}' (ID: {$pageDbId}): " . $e->getMessage();
        log_activity($pageErrorMessage, "error");
    } catch (PDOException $e) {
        $pageRunStatus = 'error';
        $pageRunMessage = 'خطأ قاعدة بيانات أثناء معالجة الصفحة.';
        $pageErrorMessage = "خطأ قاعدة بيانات عام للصفحة '{$pageName}' (ID: {$pageDbId}): " . $e->getMessage();
        log_activity($pageErrorMessage, "error");
    }

    end_page_processing:; // نقطة القفز هنا للصفحة الواحدة

    // **** تحديث حالة القفل للصفحة في قاعدة البيانات ****
    try {
        if (isset($pageDbId)) {
            $stmtReleaseLock = $pdo->prepare("UPDATE facebook_pages SET is_processing = FALSE, 
                                                                        current_active_account_id = :current_acc_id,
                                                                        last_rotation_start_time = :last_rot_time
                                              WHERE id = :page_id");
            $stmtReleaseLock->execute([
                ':page_id' => $pageDbId,
                ':current_acc_id' => $selectedAccountToProcess['account_db_id'] ?? null, // الحساب الذي كان نشطا
                ':last_rot_time' => $newRotationStartTime ?? $page['last_rotation_start_time'] // وقت بداية الدورة
            ]);
            log_activity("Released lock for page '{$pageName}' (ID: {$pageDbId}). Current active account: " . ($selectedAccountToProcess['account_name'] ?? 'None'), "info");
        }
    } catch (PDOException $e) {
        log_activity("Database error releasing lock for page '{$pageName}': " . $e->getMessage(), "error");
    }
    // **********************************

} // نهاية حلقة foreach لكل صفحة

// 13. إغلاق الاتصال بقاعدة البيانات وتسجيل نهاية الكرون جوب
unset($pdo);
log_activity("Cron job finished successfully. " . date('Y-m-d H:i:s'), "info");
?>
