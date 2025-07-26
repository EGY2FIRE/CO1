<?php
// auth_check.php
if (session_status() == PHP_SESSION_NONE) {
    session_start();
}

if (!isset($_SESSION['user_id'])) {
    header("location: login.php");
    exit();
}
?>