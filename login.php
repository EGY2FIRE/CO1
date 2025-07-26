<?php
require_once 'config.php';

$email_err = $password_err = '';

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    // Validate email
    if (empty(trim($_POST['email']))) {
        $email_err = 'Please enter your email.';
    }

    // Validate password
    if (empty(trim($_POST['password']))) {
        $password_err = 'Please enter your password.';
    }

    if (empty($email_err) && empty($password_err)) {
        $sql = "SELECT id, username, email, password_hash FROM users WHERE email = :email";

        if ($stmt = $pdo->prepare($sql)) {
            $stmt->bindParam(':email', $param_email, PDO::PARAM_STR);
            $param_email = trim($_POST['email']);

            if ($stmt->execute()) {
                if ($stmt->rowCount() == 1) {
                    $user = $stmt->fetch();
                    if (password_verify(trim($_POST['password']), $user['password_hash'])) {
                        // Password is correct, start a new session
                        session_regenerate_id(true); // لزيادة الأمان
                        $_SESSION['user_id'] = $user['id'];
                        $_SESSION['username'] = $user['username'];
                        header("location: index.php"); // توجه إلى لوحة التحكم الرئيسية
                    } else {
                        $password_err = 'The password you entered was not valid.';
                    }
                } else {
                    $email_err = 'No account found with that email.';
                }
            } else {
                echo "Oops! Something went wrong. Please try again later.";
            }
        }
        unset($stmt);
    }
}
?>

<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>تسجيل الدخول</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
    <div class="wrapper">
        <h2>تسجيل الدخول</h2>
        <form action="<?php echo htmlspecialchars($_SERVER['PHP_SELF']); ?>" method="post">
            <div class="form-group">
                <label>البريد الإلكتروني</label>
                <input type="email" name="email" class="form-control <?php echo (!empty($email_err)) ? 'is-invalid' : ''; ?>" value="<?php echo htmlspecialchars($_POST['email'] ?? ''); ?>">
                <span class="invalid-feedback"><?php echo $email_err; ?></span>
            </div>
            <div class="form-group">
                <label>كلمة المرور</label>
                <input type="password" name="password" class="form-control <?php echo (!empty($password_err)) ? 'is-invalid' : ''; ?>">
                <span class="invalid-feedback"><?php echo $password_err; ?></span>
            </div>
            <div class="form-group">
                <input type="submit" class="btn btn-primary" value="تسجيل الدخول">
            </div>
            <p>لا تمتلك حسابًا؟ <a href="register.php">سجل الآن</a>.</p>
            <p><a href="forgot_password.php">نسيت كلمة المرور؟</a></p>
        </form>
    </div>
</body>
</html>
<?php unset($pdo); ?>