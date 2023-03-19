<?php
/**
 * sudo apt install mysql-server -y
 * CREATE USER 'root'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY '123456';
 * grant all privileges on *.* to 'root'@'127.0.0.1' with grant option;
 * FLUSH PRIVILEGES;
 *
 * apt install php7.4-cli
 * apt-get install php-mysqli
 * #解决7.4版本报错
 * #vim /etc/mysql/mysql.conf.d/mysqld.cnf
 * [mysqld]
 * default_authentication_plugin = mysql_native_password
 * #重启mysql
 * service mysql restart
 */
$host = "localhost";
$username = "root";
$password = "123456";
$dbname = "test";
$orders = genOrders(2);

$conn = mysqli_connect($host, $username, $password, $dbname);
if (!$conn) {
    die("Connection failed: " . mysqli_connect_error());
}
batchSql($conn, $orders);

function batchSql($conn, $orders)
{
    foreach ($orders as $order) {
        $keyArr = array_keys($order);
        $valArr = array_values($order);

        $keys = implode(",", $keyArr);
        $vals = implode("','", $valArr);
        $sql = "insert into tb_order($keys)values('$vals') \n";

        if (mysqli_query($conn, $sql)) {
            echo "ok";
        } else {
            echo "Error: " . $sql . "<br>" . mysqli_error($conn);
        }
    }
}


function genOrders($num = 1)
{
    $orders = [];
    for ($i = 0; $i < $num; $i++) {
        $goodsId = rand(10000, 99999);
        $userId = rand(10000, 99999);
        $price = $goodsId / 1000;
        $amount = rand(1, 10);
        $couponId = rand(10000, 99999);
        $createTimestamp = time() - rand(1, 100) * 86400;

        $order = [
            'order_no' => date("YmdHis") . rand(10000, 99999) . $i,
            'third_order_no' => date("YmdHis") . rand(1000000, 9999999),
            'user_id' => $userId,
            'inviter_id' => $userId % 44444,
            'shop_id' => rand(1000, 4000),
            'goods_id' => $goodsId,
            'goods_cate_id' => intval($goodsId / 100),
            'goods_name' => "goodsName" . $goodsId,
            'price' => $price,
            'amount' => $amount,
            'total_price' => $price * $amount,
            'coupon_id' => $couponId,
            'coupon_amount' => $couponId % 9,
            'coupon_name' => "优惠劵" . $couponId,
            'pay_channel' => rand(1, 4),
            'created_time' => date("Y-m-d H:i:s", $createTimestamp),
            'status' => rand(1, 6),
            'user_remark' => "用户备注" . $userId . rand(1000, 9999),
        ];
        $order['freight_charge'] = rand(1, 5) * 10;

        $order['pay_amount'] = $order['total_price'] - $order['coupon_amount'] + $order['freight_charge'];
        $order['pay_order_no'] = "P" . rand(100000000, 900000000);
        $order['address_id'] = $userId + 30000;
        if ($order['status'] >= 2) {
            $order['pay_time'] = date("Y-m-d H:i:s", $createTimestamp + rand(10, 300));
            $order['updated_time'] = $order['pay_time'];
        }

        if ($order['status'] >= 4) {
            $order['send_out_time'] = date("Y-m-d H:i:s", strtotime($order['pay_time']) + rand(16400, 86400));
            $order['updated_time'] = $order['send_out_time'];
        }

        if ($order['status'] >= 5) {
            $order['finished_time'] = date("Y-m-d H:i:s", strtotime($order['send_out_time']) + rand(16400, 86400));
            $order['updated_time'] = $order['send_out_time'];
        }
        $orders[] = $order;
    }
    return $orders;
}

mysqli_close($conn);
?>