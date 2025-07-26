<?php
// helpers.php - ملف يحتوي على دوال مساعدة عامة

/**
 * تحويل التاريخ والوقت إلى صيغة "منذ كذا" باللغة العربية.
 *
 * @param string $datetime التاريخ والوقت (مثال: '2025-07-24 10:30:00')
 * @return string الوصف الزمني (مثال: 'منذ 5 دقائق', 'منذ يومين')
 */
function time_ago_in_arabic($datetime) {
    // التأكد من أن المنطقة الزمنية مضبوطة (تم ضبطها في config.php)
    // date_default_timezone_set('Africa/Cairo'); 

    // strtotime() سيفسر السلسلة النصية بناءً على المنطقة الزمنية الافتراضية لـ PHP (Africa/Cairo)
    $timestamp = strtotime($datetime);
    if (!$timestamp) return 'غير محدد'; // إذا كان التاريخ غير صالح

    // time() يعيد التوقيت الحالي كطابع زمني Unix (بتوقيت UTC)
    // يجب أن نحول $timestamp إلى UTC للمقارنة الصحيحة
    // بما أن $datetime تم تفسيره على أنه بتوقيت القاهرة بواسطة strtotime()، فإننا نحتاج إلى إعادته إلى UTC
    $dt_obj = new DateTime($datetime, new DateTimeZone('Africa/Cairo'));
    $dt_obj->setTimezone(new DateTimeZone('UTC'));
    $timestamp_utc = $dt_obj->getTimestamp();

    $diff = time() - $timestamp_utc; // المقارنة الآن بين توقيتين UTC

    if ($diff < 0) { // في المستقبل
        return 'في المستقبل';
    } elseif ($diff < 60) {
        return 'منذ ' . $diff . ' ثانية';
    } elseif ($diff < 3600) { // أقل من ساعة
        return 'منذ ' . round($diff / 60) . ' دقيقة';
    } elseif ($diff < 86400) { // أقل من يوم
        return 'منذ ' . round($diff / 3600) . ' ساعة';
    } elseif ($diff < 2592000) { // أقل من 30 يوم (شهر تقريباً)
        return 'منذ ' . round($diff / 86400) . ' يوم';
    } elseif ($diff < 31536000) { // أقل من سنة
        return 'منذ ' . round($diff / 2592000) . ' شهر';
    } else { // أكثر من سنة
        return 'منذ ' . round($diff / 31536000) . ' سنة';
    }
}
?>
