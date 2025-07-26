<?php
require_once 'config.php';

$username_err = $email_err = $password_err = '';

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    // Validate username
    if (empty(trim($_POST['username']))) {
        $username_err = 'Please enter a username.';
    } else {
        $sql = "SELECT id FROM users WHERE username = :username";
        if ($stmt = $pdo->prepare($sql)) {
            $stmt->bindParam(':username', $param_username, PDO::PARAM_STR);
            $param_username = trim($_POST['username']);
            if ($stmt->execute()) {
                if ($stmt->rowCount() == 1) {
                    $username_err = 'This username is already taken.';
                }
            } else {
                echo "Oops! Something went wrong. Please try again later.";
            }
        }
        unset($stmt);
    }

    // Validate email
    if (empty(trim($_POST['email']))) {
        $email_err = 'Please enter an email.';
    } elseif (!filter_var(trim($_POST['email']), FILTER_VALIDATE_EMAIL)) {
        $email_err = 'Invalid email format.';
    } else {
        $sql = "SELECT id FROM users WHERE email = :email";
        if ($stmt = $pdo->prepare($sql)) {
            $stmt->bindParam(':email', $param_email, PDO::PARAM_STR);
            $param_email = trim($_POST['email']);
            if ($stmt->execute()) {
                if ($stmt->rowCount() == 1) {
                    $email_err = 'This email is already registered.';
                }
            } else {
                echo "Oops! Something went wrong. Please try again later.";
            }
        }
        unset($stmt);
    }

    // Validate password
    if (empty(trim($_POST['password']))) {
        $password_err = 'Please enter a password.';
    } elseif (strlen(trim($_POST['password'])) < 6) {
        $password_err = 'Password must have at least 6 characters.';
    }

    // If no errors, insert into database
    if (empty($username_err) && empty($email_err) && empty($password_err)) {
        $sql = "INSERT INTO users (username, email, password_hash) VALUES (:username, :email, :password_hash)";
        if ($stmt = $pdo->prepare($sql)) {
            $stmt->bindParam(':username', $param_username, PDO::PARAM_STR);
            $stmt->bindParam(':email', $param_email, PDO::PARAM_STR);
            $stmt->bindParam(':password_hash', $param_password_hash, PDO::PARAM_STR);

            $param_username = trim($_POST['username']);
            $param_email = trim($_POST['email']);
            $param_password_hash = password_hash(trim($_POST['password']), PASSWORD_DEFAULT);

            if ($stmt->execute()) {
                header("location: login.php");
            } else {
                echo "Something went wrong. Please try again later.";
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
    <title>تسجيل حساب جديد</title>
    <link rel="stylesheet" href="style.css"> </head>
<body>
    <div class="wrapper">
        <h2>تسجيل حساب جديد</h2>
        <form action="<?php echo htmlspecialchars($_SERVER['PHP_SELF']); ?>" method="post">
            <div class="form-group">
                <label>اسم المستخدم</label>
                <input type="text" name="username" class="form-control <?php echo (!empty($username_err)) ? 'is-invalid' : ''; ?>" value="<?php echo htmlspecialchars($_POST['username'] ?? ''); ?>">
                <span class="invalid-feedback"><?php echo $username_err; ?></span>
            </div>
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
                <input type="submit" class="btn btn-primary" value="تسجيل">
            </div>
            <p>لديك حساب بالفعل؟ <a href="login.php">سجل دخولك الآن</a>.</p>
        </form>
    </div>
</body>
</html>
<?php unset($pdo); ?>