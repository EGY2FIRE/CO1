<?php
// webhook.php
// هذا السكريبت يستقبل إشعارات Webhook من فيسبوك لمعالجة التعليقات الفورية.

// 1. تضمين ملف الإعدادات وقاعدة البيانات
require_once __DIR__ . '/config.php';
require_once __DIR__ . '/path/to/facebook-php-sdk-v5/src/Facebook/autoload.php';
require_once __DIR__ . '/helpers.php'; // الدوال المساعدة

// 2. إعدادات الـ Webhook
define('WEBHOOK_VERIFY_TOKEN', 'MY_SECRET_WEBHOOK_TOKEN_123'); // يجب أن يتطابق مع الرمز الذي وضعته في إعدادات Facebook App
define('ROTATION_INTERVAL_MINUTES', 10); // مدة تبديل الحسابات بالدقائق
define('MAX_REPLIES_PER_HOUR_PER_ACCOUNT', 150); // حد الردود لكل حساب في الساعة

// 3. دالة لتسجيل الأنشطة (نفس الدالة المستخدمة في الكرون جوب)
function log_activity($message, $level = 'info') {
    $log_directory = __DIR__ . '/logs/';
    if (!is_dir($log_directory)) {
        if (!mkdir($log_directory, 0755, true)) {
            error_log("Failed to create log directory: " . $log_directory);
            error_log("[$level] $message");
            return;
        }
    }
    $log_file = $log_directory . 'webhook_activity.log'; // سجل خاص بالـ Webhook
    $timestamp = date('Y-m-d H:i:s');
    file_put_contents($log_file, "[$timestamp][$level] $message\n", FILE_APPEND);
}

log_activity("Webhook request received. " . date('Y-m-d H:i:s'), "info");

// 4. التحقق من الـ Webhook (عندما يقوم فيسبوك بالتحقق من الرابط لأول مرة)
if (isset($_GET['hub_mode']) && $_GET['hub_mode'] == 'subscribe' && isset($_GET['hub_verify_token']) && isset($_GET['hub_challenge'])) {
    if ($_GET['hub_verify_token'] == WEBHOOK_VERIFY_TOKEN) {
        echo $_GET['hub_challenge'];
        log_activity("Webhook verified successfully.", "info");
        exit();
    } else {
        header('HTTP/1.0 403 Forbidden');
        log_activity("Webhook verification failed: Invalid verify token.", "error");
        exit();
    }
}

// 5. معالجة إشعارات الـ Webhook (عندما يرسل فيسبوك بيانات)
$input = file_get_contents('php://input');
$data = json_decode($input, true);

if (empty($data)) {
    log_activity("Received empty or invalid Webhook data.", "warning");
    exit();
}

// تأكد من أن الطلب من فيسبوك ومن نوع "page"
if (isset($data['object']) && $data['object'] == 'page' && isset($data['entry'])) {
    foreach ($data['entry'] as $entry) {
        if (isset($entry['changes'])) {
            foreach ($entry['changes'] as $change) {
                if ($change['field'] == 'feed' || $change['field'] == 'comments') {
                    // تحقق من أن التغيير هو تعليق جديد
                    if (isset($change['value']) && $change['value']['item'] == 'comment' && $change['value']['verb'] == 'add') {
                        $comment_data = $change['value'];
                        $commentId = $comment_data['comment_id'];
                        $postId = $comment_data['post_id'];
                        $pageIdFb = $entry['id']; // ID الصفحة على فيسبوك
                        $commentMessage = $comment_data['message'];
                        $commentCreatorId = $comment_data['from']['id'];
                        $commentCreatorName = $comment_data['from']['name'];
                        $commentCreatedTime = date('Y-m-d H:i:s', $comment_data['created_time']); // وقت التعليق من فيسبوك

                        log_activity("New comment received. Comment ID: {$commentId}, Page FB ID: {$pageIdFb}, From: {$commentCreatorName}", "info");

                        // 6. تهيئة Facebook SDK (لإرسال الرد)
                        try {
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
                        } catch (Exception $e) {
                            log_activity("Failed to initialize Facebook SDK for reply: " . $e->getMessage(), "critical");
                            continue; // انتقل للتغيير التالي
                        }

                        // 7. جلب معلومات الصفحة من قاعدة البيانات
                        $stmtPage = $pdo->prepare("SELECT fp.id AS page_db_id, fp.user_id AS page_owner_user_id, fp.current_rotation_account_id, fp.last_rotation_time
                                                   FROM facebook_pages fp WHERE fp.page_id_fb = :page_id_fb");
                        $stmtPage->execute([':page_id_fb' => $pageIdFb]);
                        $page = $stmtPage->fetch();

                        if (!$page) {
                            log_activity("Page with FB ID {$pageIdFb} not found in database. Skipping.", "warning");
                            continue;
                        }

                        $pageDbId = $page['page_db_id'];
                        $pageOwnerUserId = $page['page_owner_user_id'];
                        $currentRotationAccountId = $page['current_rotation_account_id'];
                        $lastRotationTime = $page['last_rotation_time'];

                        $selectedAccount = null; // الحساب الذي سيتم استخدامه للرد

                        // 8. منطق تبديل الحسابات (Rotation Logic)
                        $rotateNow = false;
                        if (empty($currentRotationAccountId)) {
                            $rotateNow = true; // لا يوجد حساب نشط حاليا، اختر الأول
                            log_activity("No current rotation account set for page {$pageDbId}. Initializing rotation.", "debug");
                        } else {
                            $lastRotationDateTime = new DateTime($lastRotationTime, new DateTimeZone('UTC'));
                            $nowDateTime = new DateTime('now', new DateTimeZone('UTC'));
                            $interval = $nowDateTime->getTimestamp() - $lastRotationDateTime->getTimestamp(); // الفارق بالثواني

                            if ($interval >= (ROTATION_INTERVAL_MINUTES * 60)) {
                                $rotateNow = true; // حان وقت التبديل
                                log_activity("Rotation interval (10 mins) passed for page {$pageDbId}. Rotating account.", "debug");
                            } else {
                                // حاول استخدام الحساب الحالي، وتأكد من أنه لا يزال نشطًا
                                $stmtCurrentAccount = $pdo->prepare("SELECT * FROM facebook_page_accounts WHERE id = :id AND is_active = TRUE");
                                $stmtCurrentAccount->execute([':id' => $currentRotationAccountId]);
                                $selectedAccount = $stmtCurrentAccount->fetch();
                                if (!$selectedAccount) {
                                    $rotateNow = true; // الحساب الحالي غير نشط، يجب التبديل
                                    log_activity("Current rotation account {$currentRotationAccountId} for page {$pageDbId} is inactive. Rotating.", "debug");
                                }
                            }
                        }

                        if ($rotateNow) {
                            $stmtActiveAccounts = $pdo->prepare("SELECT id FROM facebook_page_accounts WHERE page_id = :page_id AND is_active = TRUE ORDER BY id ASC");
                            $stmtActiveAccounts->execute([':page_id' => $pageDbId]);
                            $activeAccounts = $stmtActiveAccounts->fetchAll(PDO::FETCH_COLUMN);

                            if (empty($activeAccounts)) {
                                log_activity("No active accounts found for page {$pageDbId} to rotate to. Skipping reply.", "warning");
                                continue;
                            }

                            $nextAccountIndex = 0;
                            if (!empty($currentRotationAccountId) && in_array($currentRotationAccountId, $activeAccounts)) {
                                $currentIndex = array_search($currentRotationAccountId, $activeAccounts);
                                $nextAccountIndex = ($currentIndex + 1) % count($activeAccounts);
                            }
                            $nextRotationAccountId = $activeAccounts[$nextAccountIndex];

                            // تحديث الصفحة بالحساب الجديد ووقت التبديل
                            $stmtUpdatePageRotation = $pdo->prepare("UPDATE facebook_pages SET current_rotation_account_id = :account_id, last_rotation_time = UTC_TIMESTAMP() WHERE id = :page_id");
                            $stmtUpdatePageRotation->execute([
                                ':account_id' => $nextRotationAccountId,
                                ':page_id' => $pageDbId
                            ]);
                            log_activity("Rotated page {$pageDbId} to account ID {$nextRotationAccountId}.", "info");

                            // جلب تفاصيل الحساب الجديد
                            $stmtSelectedAccount = $pdo->prepare("SELECT * FROM facebook_page_accounts WHERE id = :id");
                            $stmtSelectedAccount->execute([':id' => $nextRotationAccountId]);
                            $selectedAccount = $stmtSelectedAccount->fetch();

                        } // نهاية منطق التبديل if ($rotateNow)

                        if (!$selectedAccount) {
                            log_activity("Failed to select an active account for page {$pageDbId} after rotation logic. Skipping reply.", "error");
                            continue;
                        }

                        $dbPageAccountId = $selectedAccount['id'];
                        $accountName = $selectedAccount['account_name'];
                        $pageAccessToken = $selectedAccount['access_token'];
                        $enableReactions = $selectedAccount['enable_reactions'];
                        $reactionType = $selectedAccount['reaction_type'];

                        // 9. التحقق من حد الردود في الساعة (Rate Limit)
                        $repliesLastHour = get_replies_count_realtime($pdo, $dbPageAccountId, 1); // دالة جديدة لحساب الردود في الساعة
                        if ($repliesLastHour >= MAX_REPLIES_PER_HOUR_PER_ACCOUNT) {
                            log_activity("Account {$accountName} (ID: {$dbPageAccountId}) reached hourly reply limit ({$repliesLastHour} replies). Skipping reply to comment {$commentId}.", "warning");
                            continue;
                        }

                        // 10. التحقق من أن التعليق لم يتم الرد عليه بعد (منع التكرار)
                        $stmtCheck = $pdo->prepare("SELECT id FROM processed_comments WHERE comment_id_fb = :comment_id AND page_id = :page_id");
                        $stmtCheck->execute([':comment_id' => $commentId, ':page_id' => $pageDbId]);

                        if ($stmtCheck->rowCount() > 0) {
                            log_activity("Comment ID {$commentId} on page '{$pageDbId}' already processed. Skipping.", "debug");
                            continue; // تم الرد عليه بالفعل، تخطى
                        }

                        // 11. جلب قالب الرد (نفس منطق الأولوية)
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
                                        log_activity("No active templates (page-specific or global) found for page {$pageDbId}. Cannot reply to comment {$commentId}.", "warning");
                                        continue;
                                    }
                                }
                            }
                        } catch (PDOException $e) {
                            log_activity("Database error when fetching templates for page {$pageDbId}: " . $e->getMessage(), "error");
                            continue;
                        }

                        if (empty($templates_contents)) {
                            log_activity("No templates available after all checks for page {$pageDbId}. Cannot reply to comment {$commentId}.", "error");
                            continue;
                        }

                        $randomTemplateContent = $templates_contents[array_rand($templates_contents)];
                        $replyMessage = str_replace('{user_name}', htmlspecialchars($commentCreatorName), $randomTemplateContent);

                        log_activity("Attempting to reply to comment ID {$commentId} from '{$commentCreatorName}' on page '{$pageIdFb}' via account '{$accountName}'.", "info");

                        // 12. إرسال الرد
                        try {
                            $response = $fb->post(
                                "/{$commentId}/comments",
                                ['message' => $replyMessage],
                                $pageAccessToken
                            );
                            $graphNode = $response->getGraphNode();
                            log_activity("Successfully replied to comment ID {$commentId}. Reply ID: " . $graphNode->getField('id'), "success");

                            // 13. تسجيل الرد في قاعدة البيانات
                            $stmtInsert = $pdo->prepare("INSERT IGNORE INTO processed_comments (comment_id_fb, page_id, fb_page_account_id) VALUES (:comment_id, :page_id, :fb_page_account_id)");
                            $stmtInsert->execute([
                                ':comment_id' => $commentId,
                                ':page_id' => $pageDbId,
                                ':fb_page_account_id' => $dbPageAccountId
                            ]);

                            // 14. تحديث إحصائيات الردود الساعية
                            $current_hour_utc = (new DateTime('now', new DateTimeZone('UTC')))->format('Y-m-d H:00:00');
                            $stmtUpdateHourlyStats = $pdo->prepare("INSERT INTO hourly_reply_stats (fb_page_account_id, hour_timestamp, reply_count) 
                                                                    VALUES (:account_id, :hour_ts, 1)
                                                                    ON DUPLICATE KEY UPDATE reply_count = reply_count + 1");
                            $stmtUpdateHourlyStats->execute([
                                ':account_id' => $dbPageAccountId,
                                ':hour_ts' => $current_hour_utc
                            ]);

                            // 15. منطق الإعجاب بالتعليق (إذا لم يتم الرد عليه بعد)
                            if ($enableReactions) { // نفعل الإعجاب فقط إذا كان مفعلاً
                                try {
                                    $reactionEndpoint = "/{$commentId}/reactions";
                                    $reactionData = ['type' => strtoupper($reactionType)];
                                    log_activity("Attempting to react with '{$reactionType}' to comment ID {$commentId}.", "info");
                                    $fb->post($reactionEndpoint, $reactionData, $pageAccessToken);
                                    log_activity("Successfully reacted with '{$reactionType}' to comment ID {$commentId}.", "success");
                                } catch (FacebookResponseException $e) {
                                    log_activity("Graph API error when reacting to comment ID {$commentId}: " . $e->getMessage(), "error");
                                } catch (FacebookSDKException $e) {
                                    log_activity("Facebook SDK error when reacting to comment ID {$commentId}: " . $e->getMessage(), "error");
                                }
                            }

                        } catch (FacebookResponseException $e) {
                            log_activity("Graph API error when replying to comment {$commentId}: " . $e->getMessage(), "error");
                        } catch (FacebookSDKException $e) {
                            log_activity("Facebook SDK error when replying to comment {$commentId}: " . $e->getMessage(), "error");
                        }

                    } // نهاية if (comment is new and not processed)
                } // نهاية if (comment item and verb)
            } // نهاية foreach changes
        } // نهاية if (changes)
    } // نهاية foreach entry
} else {
    log_activity("Received non-page object or no entries. Data: " . $input, "debug");
}

// 16. إغلاق الاتصال بقاعدة البيانات
unset($pdo);
log_activity("Webhook request processed successfully. " . date('Y-m-d H:i:s'), "info");
?>
