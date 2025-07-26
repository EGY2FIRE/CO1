<?php
require_once 'auth_check.php'; // تأكد من تسجيل دخول المستخدم
require_once 'config.php';

require_once 'path/to/facebook-php-sdk-v5/src/Facebook/autoload.php'; // استبدل بالمسار الصحيح

$fb = new Facebook\Facebook([
    'app_id' => FB_APP_ID,
    'app_secret' => FB_APP_SECRET,
    'default_graph_version' => FB_GRAPH_VERSION,
]);

$helper = $fb->getRedirectLoginHelper();

try {
    $accessToken = $helper->getAccessToken();
} catch (Facebook\Exception\FacebookResponseException $e) {
    echo 'Graph returned an error: ' . $e->getMessage();
    exit;
} catch (Facebook\Exception\FacebookSDKException $e) {
    echo 'Facebook SDK returned an error: ' . $e->getMessage();
    exit;
}

if (!isset($accessToken)) {
    if ($helper->getError()) {
        header('HTTP/1.0 401 Unauthorized');
        echo "Error: " . $helper->getError() . "\n";
        exit;
    } else {
        header('HTTP/1.0 400 Bad Request');
        echo 'Bad request';
        exit();
    }
}

// الـ Access Token للمستخدم. يجب تحويله إلى Long-Lived Access Token.
$oAuth2Client = $fb->getOAuth2Client();
try {
    $longLivedAccessToken = $oAuth2Client->getLongLivedAccessToken($accessToken);
    $tokenExpiration = $longLivedAccessToken->getExpiresAt() ? $longLivedAccessToken->getExpiresAt()->format('Y-m-d H:i:s') : null;
} catch (Facebook\Exception\FacebookSDKException $e) {
    echo 'Error getting long-lived access token: ' . $e->getMessage();
    exit;
}

// الحصول على معلومات المستخدم نفسه (للحصول على اسم الحساب الـ FB ID)
try {
    $responseUser = $fb->get('/me?fields=id,name', $longLivedAccessToken);
    $graphUser = $responseUser->getGraphUser();
    $facebookUserId = $graphUser->getId();
    $facebookUserName = $graphUser->getName();
} catch (Facebook\Exception\FacebookResponseException $e) {
    echo 'Graph returned an error getting user info: ' . $e->getMessage();
    exit;
} catch (Facebook\Exception\FacebookSDKException $e) {
    echo 'Facebook SDK returned an error getting user info: ' . $e->getMessage();
    exit;
}

// **** جلب الإعدادات الافتراضية للمستخدم ****
$stmtUserSettings = $pdo->prepare("SELECT default_reply_interval_seconds, default_comments_limit_per_run, default_custom_since_hours, rotation_duration_minutes, max_replies_per_hour FROM users WHERE id = :user_id");
$stmtUserSettings->execute([':user_id' => $_SESSION['user_id']]);
$userDefaults = $stmtUserSettings->fetch();

$defaultReplyInterval = $userDefaults['default_reply_interval_seconds'] ?? 30;
$defaultCommentsLimit = $userDefaults['default_comments_limit_per_run'] ?? 10;
$defaultCustomSince = $userDefaults['default_custom_since_hours'] ?? 24;
$defaultRotationDuration = $userDefaults['rotation_duration_minutes'] ?? 10;
$defaultMaxRepliesPerHour = $userDefaults['max_replies_per_hour'] ?? 150;
// ****************************************

// الآن استخدم الـ Long-Lived User Access Token للحصول على قائمة الصفحات والـ Page Access Tokens
try {
    $responsePages = $fb->get('/me/accounts?fields=id,name,access_token,link', $longLivedAccessToken);
    $pagesEdge = $responsePages->getGraphEdge();

    foreach ($pagesEdge as $page) {
        $pageIdFb = $page->getField('id');
        $pageName = $page->getField('name');
        $pageAccessToken = $page->getField('access_token');
        $pageLink = $page->getField('link');

        // أولاً: تحقق من وجود الصفحة في جدول facebook_pages
        $stmtPage = $pdo->prepare("SELECT id FROM facebook_pages WHERE page_id_fb = :page_id_fb");
        $stmtPage->execute([':page_id_fb' => $pageIdFb]);
        $existingPage = $stmtPage->fetch();

        $dbPageId;
        if ($existingPage) {
            // الصفحة موجودة، استخدم معرفها
            $dbPageId = $existingPage['id'];
        } else {
            // الصفحة غير موجودة، قم بإضافتها إلى facebook_pages
            $stmtInsertPage = $pdo->prepare("INSERT INTO facebook_pages (user_id, page_id_fb, page_name, page_url) VALUES (:user_id, :page_id_fb, :page_name, :page_url)");
            $stmtInsertPage->execute([
                ':user_id' => $_SESSION['user_id'],
                ':page_id_fb' => $pageIdFb,
                ':page_name' => $pageName,
                ':page_url' => $pageLink
            ]);
            $dbPageId = $pdo->lastInsertId();
        }

        // ثانياً: تحقق من وجود هذا الحساب (المستخدم) الذي قام بربط هذه الصفحة
        // في جدول facebook_page_accounts (للسماح بربط نفس الصفحة بأكثر من حساب)
        $stmtAccount = $pdo->prepare("SELECT id FROM facebook_page_accounts WHERE page_id = :page_id AND user_id = :user_id AND account_id_fb = :account_id_fb");
        $stmtAccount->execute([
            ':page_id' => $dbPageId,
            ':user_id' => $_SESSION['user_id'],
            ':account_id_fb' => $facebookUserId
        ]);
        $existingAccount = $stmtAccount->fetch();

        if ($existingAccount) {
            // هذا الحساب مرتبط بهذه الصفحة بالفعل، قم بالتحديث
            $stmtUpdateAccount = $pdo->prepare("UPDATE facebook_page_accounts SET
                account_name = :account_name,
                access_token = :access_token,
                token_expiry_date = :token_expiry_date,
                updated_at = NOW()
                WHERE id = :id");
            $stmtUpdateAccount->execute([
                ':account_name' => $facebookUserName,
                ':access_token' => $pageAccessToken,
                ':token_expiry_date' => $tokenExpiration,
                ':id' => $existingAccount['id']
            ]);
        } else {
            // هذا الحساب لم يتم ربطه بهذه الصفحة بعد، قم بالإضافة
            $stmtInsertAccount = $pdo->prepare("INSERT INTO facebook_page_accounts
                (page_id, user_id, account_id_fb, account_name, access_token, token_expiry_date,
                 reply_interval_seconds, comments_limit_per_run, custom_since_hours,
                 rotation_duration_minutes, max_replies_per_hour)
                VALUES (:page_id, :user_id, :account_id_fb, :account_name, :access_token, :token_expiry_date,
                        :reply_interval_seconds, :comments_limit_per_run, :custom_since_hours,
                        :rotation_duration_minutes, :max_replies_per_hour)");
            $stmtInsertAccount->execute([
                ':page_id' => $dbPageId,
                ':user_id' => $_SESSION['user_id'],
                ':account_id_fb' => $facebookUserId,
                ':account_name' => $facebookUserName,
                ':access_token' => $pageAccessToken,
                ':token_expiry_date' => $tokenExpiration,
                ':reply_interval_seconds' => $defaultReplyInterval,
                ':comments_limit_per_run' => $defaultCommentsLimit,
                ':custom_since_hours' => $defaultCustomSince,
                ':rotation_duration_minutes' => $defaultRotationDuration,
                ':max_replies_per_hour' => $defaultMaxRepliesPerHour
            ]);
        }
    }

    header("location: pages.php"); // توجه إلى صفحة إدارة الصفحات
    exit();

} catch (Facebook\Exception\FacebookResponseException $e) {
    echo 'Graph returned an error: ' . $e->getMessage();
    exit;
} catch (Facebook\Exception\FacebookSDKException $e) {
    echo 'Facebook SDK returned an error: ' . $e->getMessage();
    exit;
}
?>
