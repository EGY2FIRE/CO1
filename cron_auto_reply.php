
<?php
// cron_auto_reply.php
// هذا السكريبت مصمم للتشغيل بواسطة Cron Job بشكل متكرر (مثلاً كل 5-10 دقائق)

// 1. تضمين ملف الإعدادات والاتصال بقاعدة البيانات
// نفترض أن config.php موجود في المجلد الأب (المجلد الرئيسي للمشروع)
require_once __DIR__ . '/config.php';

// 2. تضمين Facebook SDK
// نفترض أن مجلد 'vendor' (الناتج عن Composer) موجود في المجلد الأب (المجلد الرئيسي للمشروع)
require_once __DIR__ . '/path/to/facebook-php-sdk-v5/src/Facebook/autoload.php';

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


// 5. جلب جميع ارتباطات الصفحات بالحسابات المفعلة من قاعدة البيانات
try {
    $stmt = $pdo->prepare("SELECT fpa.*, fp.page_name, fp.page_id_fb, fp.user_id AS page_owner_user_id
                           FROM facebook_page_accounts fpa
                           JOIN facebook_pages fp ON fpa.page_id = fp.id
                           WHERE fpa.is_active = TRUE");
    $stmt->execute();
    $activePageAccounts = $stmt->fetchAll();
    log_activity("Found " . count($activePageAccounts) . " active page-account links to process.", "info");

} catch (PDOException $e) {
    log_activity("Database error when fetching active page-account links: " . $e->getMessage(), "error");
    unset($pdo);
    exit();
}


// 6. تكرار على كل ربط حساب-صفحة ومعالجة التعليقات
foreach ($activePageAccounts as $pageAccount) {
    // إضافة تأخير عشوائي بالمللي ثانية لتقليل فرص التضارب إذا كانت العمليات متوازية
    usleep(mt_rand(1000000, 3000000)); // تأخير من 1 إلى 3 ثواني

    $dbPageAccountId = $pageAccount['id']; // ID هذا الربط في قاعدة البيانات (facebook_page_accounts.id)
    $dbPageId = $pageAccount['page_id']; // ID الصفحة في قاعدة البيانات (facebook_pages.id)
    $pageIdFb = $pageAccount['page_id_fb']; // ID الصفحة على فيسبوك
    $pageName = $pageAccount['page_name'];
    $accountName = $pageAccount['account_name']; // اسم الحساب المربوط
    $pageAccessToken = $pageAccount['access_token']; // Access Token الخاص بهذا الربط
    $lastProcessedTime = $pageAccount['last_processed_comment_time']; // آخر وقت تم فيه معالجة تعليق بواسطة هذا الربط (للتتبع فقط)
    $replyIntervalSeconds = $pageAccount['reply_interval_seconds'] ?? 30; // الفاصل الزمني بين الردود لهذا الربط
    $commentsLimit = $pageAccount['comments_limit_per_run'] ?? 10; // حد التعليقات للمعالجة في كل جولة لهذا الربط
    $pageOwnerUserId = $pageAccount['page_owner_user_id']; // معرف المستخدم الذي يمتلك هذه الصفحة
    $customSinceHours = $pageAccount['custom_since_hours']; // المدة المخصصة للبحث (بالساعات)
    $enableReactions = $pageAccount['enable_reactions']; // تفعيل الإعجابات
    $reactionType = $pageAccount['reaction_type'];       // نوع الإعجاب

    log_activity("Processing page: " . $pageName . " (FB ID: " . $pageIdFb . ", Account: " . $accountName . ", Last Processed: " . $lastProcessedTime . ")", "info");

    // تحديد نطاق زمني ثابت لجلب التعليقات بناءً على custom_since_hours
    $fetchSinceHours = 24; 
    if ($customSinceHours !== null && is_numeric($customSinceHours) && $customSinceHours >= 0) {
        $fetchSinceHours = $customSinceHours;
    }
    // إذا كانت القيمة 0، فهذا يعني "منذ البداية" (epoch)
    $fetchSinceTimestamp = ($fetchSinceHours === 0) ? 0 : strtotime('-' . $fetchSinceHours . ' hours'); 
    log_activity("Fetching comments since: " . date('Y-m-d H:i:s', $fetchSinceTimestamp) . " (last " . ($fetchSinceHours === 0 ? 'all time' : $fetchSinceHours . ' hours') . ") for account '{$accountName}' on page '{$pageName}'.", "debug");
    
    // تهيئة متغيرات حالة التشغيل لهذا الحساب
    $runStatus = 'success';
    $runMessage = 'تمت المعالجة بنجاح.';
    $errorMessage = null;
    $commentsRepliedInRun = 0; // عداد للتعليقات التي تم الرد عليها في هذه الجولة
    $commentsReactedInRun = 0; // عداد للتعليقات التي تم الإعجاب بها في هذه الجولة

    // جلب قوالب الردود (منطق الأولوية)
    $templates_contents = [];
    try {
        // 1. محاولة جلب قوالب الردود المخصصة لهذه الصفحة أولاً
        $stmtPageTemplates = $pdo->prepare("SELECT rtc.content FROM reply_templates_contents rtc JOIN reply_templates rt ON rtc.template_id = rt.id WHERE rt.page_id = :page_id AND rt.is_active = TRUE");
        $stmtPageTemplates->execute([':page_id' => $dbPageId]);
        $templates_contents = $stmtPageTemplates->fetchAll(PDO::FETCH_COLUMN);

        if (empty($templates_contents)) {
            log_activity("No active page-specific templates found for page ID: " . $dbPageId . " (page: " . $pageName . "). Attempting to fetch global templates.", "info");
            
            // 2. إذا لم توجد قوالب مخصصة للصفحة، جلب القالب العام الافتراضي للمستخدم مالك الصفحة
            $stmtDefaultGlobalTemplate = $pdo->prepare("SELECT grtc.content FROM global_reply_template_contents grtc JOIN global_reply_templates grt ON grtc.template_id = grt.id WHERE grt.user_id = :user_id AND grt.is_active = TRUE AND grt.is_default = TRUE");
            $stmtDefaultGlobalTemplate->execute([':user_id' => $pageOwnerUserId]);
            $templates_contents = $stmtDefaultGlobalTemplate->fetchAll(PDO::FETCH_COLUMN);

            if (empty($templates_contents)) {
                log_activity("No active DEFAULT global templates found for user ID: " . $pageOwnerUserId . " (for page: " . $pageName . ", account: " . $accountName . "). Attempting to fetch ANY active global templates.", "warning");
                
                // 3. إذا لم يوجد قالب عام افتراضي، جلب أي قوالب عامة نشطة لهذا المستخدم
                $stmtAnyGlobalTemplate = $pdo->prepare("SELECT grtc.content FROM global_reply_template_contents grtc JOIN global_reply_templates grt ON grtc.template_id = grt.id WHERE grt.user_id = :user_id AND grt.is_active = TRUE");
                $stmtAnyGlobalTemplate->execute([':user_id' => $pageOwnerUserId]);
                $templates_contents = $stmtAnyGlobalTemplate->fetchAll(PDO::FETCH_COLUMN);

                if (empty($templates_contents)) {
                    log_activity("No active global templates (default or otherwise) found for user ID: " . $pageOwnerUserId . " (for page: " . $pageName . ", account: " . $accountName . "). Skipping this page-account link.", "warning");
                    $runStatus = 'error';
                    $runMessage = 'لا توجد قوالب ردود نشطة.';
                    $errorMessage = 'لم يتم العثور على قوالب ردود نشطة مخصصة للصفحة ولا قوالب عامة.';
                    goto end_account_processing;
                } else {
                    log_activity("Using ANY active global templates for user ID: " . $pageOwnerUserId . " on page: " . $pageName . " (no default found).", "info");
                }
            } else {
                log_activity("Using DEFAULT global templates for user ID: " . $pageOwnerUserId . " on page: " . $pageName . ".", "info");
            }
        } else {
            log_activity("Using page-specific templates for page ID: " . $dbPageId . " (page: " . $pageName . ").", "info");
        }
    } catch (PDOException $e) {
        $runStatus = 'error';
        $runMessage = 'خطأ في جلب القوالب.';
        $errorMessage = "خطأ قاعدة بيانات عند جلب القوالب: " . $e->getMessage();
        log_activity($errorMessage, "error");
        goto end_account_processing;
    }

    $currentLatestCommentTime = $lastProcessedTime; // متغير لتتبع أحدث وقت تعليق تم الرد عليه في هذه الجولة


    try {
        // 7. جلب أحدث المنشورات ثم تعليقاتها
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
                    // **** تصحيح المنطقة الزمنية هنا: عرض الأوقات بتوقيت UTC في الـ Log ****
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
                
                // **** تصحيح المنطقة الزمنية هنا: تحويل وقت التعليق إلى UTC للتخزين والمقارنة ****
                $commentDateTime = $comment->getField('created_time');
                $commentDateTime->setTimezone(new DateTimeZone('UTC')); // تأكد أن هذا الكائن بتوقيت UTC
                $commentCreatedTime = $commentDateTime->format('Y-m-d H:i:s'); // هذا هو الآن سلسلة نصية بتوقيت UTC
                // **************************************************************************

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
                        $runStatus = 'error'; 
                        $runMessage = 'لا توجد قوالب ردود نشطة للرد.';
                        $errorMessage = "لا توجد قوالب ردود نشطة للرد على التعليق {$commentId}.";
                        goto end_account_processing; 
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
                            ':fb_page_account_id' => $dbPageAccountId
                        ]);

                        log_activity("Successfully replied to comment ID {$commentId} on page '{$pageName}' via account '{$accountName}'. Reply ID: " . $graphNode->getField('id'), "success");

                        $commentsRepliedInRun++; 
                        // **** تحديث $currentLatestCommentTime باستخدام وقت التعليق بتوقيت UTC ****
                        if (strtotime($commentCreatedTime) > strtotime($currentLatestCommentTime)) {
                            $currentLatestCommentTime = $commentCreatedTime; // هذا هو الآن بتوقيت UTC
                        }
                        // ******************************************************************

                        sleep($replyIntervalSeconds);

                    } catch (FacebookResponseException $e) {
                        $runStatus = 'error';
                        $runMessage = 'خطأ في الرد على تعليق.';
                        $errorMessage = "خطأ Graph API عند الرد على تعليق {$commentId}: " . $e->getMessage();
                        log_activity($errorMessage, "error");
                        goto end_account_processing;
                    } catch (FacebookSDKException $e) {
                        $runStatus = 'error';
                        $runMessage = 'خطأ في Facebook SDK.';
                        $errorMessage = "خطأ Facebook SDK عند الرد على تعليق {$commentId}: " . $e->getMessage();
                        log_activity($errorMessage, "error");
                        goto end_account_processing;
                    }

                    if ($commentsRepliedInRun >= $commentsLimit) {
                        log_activity("Reached comments limit ({$commentsLimit}) for account '{$accountName}' on page '{$pageName}'. Stopping further processing for this link in current run.", "info");
                        $runMessage = "تم الرد على {$commentsRepliedInRun} تعليقات.";
                        goto end_account_processing;
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
                        $commentsReactedInRun++;
                        log_activity("Successfully reacted with '{$reactionType}' to comment ID {$commentId}.", "success");
                        sleep(1);
                    } catch (FacebookResponseException $e) {
                        log_activity("Graph API error when reacting to comment ID {$commentId}: " . $e->getMessage(), "error");
                    } catch (FacebookSDKException $e) {
                        log_activity("Facebook SDK error when reacting to comment ID {$commentId}: " . $e->getMessage(), "error");
                    }
                }
                // ******************************

            } // نهاية foreach التعليقات
            
            if ($commentsRepliedInRun >= $commentsLimit) {
                $runMessage = "تم الرد على {$commentsRepliedInRun} تعليقات.";
                break;
            }
        } // نهاية foreach المنشورات

        // **** تحديث إحصائيات الردود الساعية (بتوقيت UTC) ****
        if ($commentsRepliedInRun > 0) {
            $current_hour_utc = (new DateTime('now', new DateTimeZone('UTC')))->format('Y-m-d H:00:00'); // تقريب الوقت إلى بداية الساعة بتوقيت UTC
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
        // ************************************

        if ($commentsRepliedInRun == 0 && $runStatus == 'success') {
            $runMessage = 'لم يتم العثور على تعليقات جديدة.';
            $runStatus = 'no_new_comments';
        } else if ($runStatus == 'success') {
            $runMessage = "تم الرد على {$commentsRepliedInRun} تعليقات.";
            if ($commentsReactedInRun > 0) {
                $runMessage .= " وتم الإعجاب بـ {$commentsReactedInRun} تعليقات.";
            }
        }

    } catch (FacebookResponseException $e) {
        $runStatus = 'error';
        $runMessage = 'خطأ في جلب المنشورات/التعليقات.';
        $errorMessage = "خطأ Graph API عند جلب المنشورات/التعليقات لصفحة '{$pageName}': " . $e->getMessage();
        log_activity($errorMessage, "error");
    } catch (FacebookSDKException $e) {
        $runStatus = 'error';
        $runMessage = 'خطأ في Facebook SDK.';
        $errorMessage = "خطأ Facebook SDK عند جلب المنشورات/التعليقات لصفحة '{$pageName}': " . $e->getMessage();
        log_activity($errorMessage, "error");
    } catch (PDOException $e) {
        $runStatus = 'error';
        $runMessage = 'خطأ قاعدة بيانات أثناء المعالجة.';
        $errorMessage = "خطأ قاعدة بيانات أثناء معالجة التعليقات لصفحة '{$pageName}': " . $e->getMessage();
        log_activity($errorMessage, "error");
    }

    end_account_processing:; // نقطة القفز بـ goto

    // **** تحديث إحصائيات التشغيل في قاعدة البيانات ****
    $stmtUpdateStats = $pdo->prepare("UPDATE facebook_page_accounts SET 
        last_run_status = :status, 
        last_run_message = :message, 
        last_error_message = :error_msg,
        last_processed_comment_time = :last_time_processed
        WHERE id = :db_page_account_id");
    
    $stmtUpdateStats->execute([
        ':status' => $runStatus,
        ':message' => $runMessage,
        ':error_msg' => $errorMessage,
        ':last_time_processed' => ($runStatus == 'success' && $commentsRepliedInRun > 0) ? $currentLatestCommentTime : $pageAccount['last_processed_comment_time'], // تحديث فقط لو تم الرد على تعليق جديد (بتوقيت UTC)
        ':db_page_account_id' => $dbPageAccountId
    ]);
    log_activity("Updated stats for account '{$accountName}' on page '{$pageName}': Status: {$runStatus}, Message: '{$runMessage}'", "info");

} // نهاية حلقة foreach لكل حساب

// 13. إغلاق الاتصال بقاعدة البيانات وتسجيل نهاية الكرون جوب
unset($pdo);
log_activity("Cron job finished successfully. " . date('Y-m-d H:i:s'), "info");
?>
