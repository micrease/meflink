
# Flink Data CDC

## 1,安装

```agsl

CREATE TABLE `tb_order_0` (
  `id` int NOT NULL AUTO_INCREMENT,
  `order_no` varchar(32) NOT NULL DEFAULT '',
  `third_order_no` varchar(32) NOT NULL DEFAULT '',
  `user_id` int NOT NULL DEFAULT '0',
  `inviter_id` int NOT NULL DEFAULT '0',
  `shop_id` int NOT NULL DEFAULT '0',
  `goods_id` int NOT NULL DEFAULT '0',
  `goods_cate_id` int NOT NULL DEFAULT '0',
  `goods_name` varchar(64) NOT NULL DEFAULT '',
  `price` decimal(14,2) NOT NULL DEFAULT '0.00',
  `amount` int NOT NULL DEFAULT '0',
  `total_price` decimal(14,2) NOT NULL DEFAULT '0.00',
  `created_time` timestamp NULL DEFAULT NULL,
  `coupon_name` varchar(64) NOT NULL DEFAULT '',
  `pay_channel` int NOT NULL DEFAULT '0',
  `coupon_id` int NOT NULL DEFAULT '0',
  `status` int NOT NULL DEFAULT '0',
  `user_remark` varchar(256) NOT NULL DEFAULT '',
  `freight_charge` decimal(14,2) NOT NULL DEFAULT '0.00',
  `pay_amount` decimal(14,2) NOT NULL DEFAULT '0.00',
  `pay_order_no` varchar(32) NOT NULL DEFAULT '',
  `address_id` int NOT NULL DEFAULT '0',
  `pay_time` timestamp NULL DEFAULT NULL,
  `updated_time` timestamp NULL DEFAULT NULL,
  `send_out_time` timestamp NULL DEFAULT NULL,
  `finished_time` timestamp NULL DEFAULT NULL,
  `coupon_amount` decimal(14,2) NOT NULL DEFAULT '0.00',
  `order_month` int NOT NULL DEFAULT '0',
  `test1` int DEFAULT '0',
  `test2` int DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=30778 DEFAULT CHARSET=utf8mb4;
```


```sql
CREATE TABLE `tb_order` (
  `id` int NOT NULL AUTO_INCREMENT,
  `order_no` varchar(32) NOT NULL DEFAULT '',
  `third_order_no` varchar(32) NOT NULL DEFAULT '',
  `user_id` int NOT NULL DEFAULT '0',
  `inviter_id` int NOT NULL DEFAULT '0',
  `shop_id` int NOT NULL DEFAULT '0',
  `goods_id` int NOT NULL DEFAULT '0',
  `goods_cate_id` int NOT NULL DEFAULT '0',
  `goods_name` varchar(64) NOT NULL DEFAULT '',
  `price` decimal(14,2) NOT NULL DEFAULT '0.00',
  `amount` int NOT NULL DEFAULT '0',
  `total_price` decimal(14,2) NOT NULL DEFAULT '0.00',
  `created_time` timestamp NULL DEFAULT NULL,
  `coupon_name` varchar(64) NOT NULL DEFAULT '',
  `pay_channel` int NOT NULL DEFAULT '0',
  `coupon_id` int NOT NULL DEFAULT '0',
  `status` int NOT NULL DEFAULT '0',
  `user_remark` varchar(256) NOT NULL DEFAULT '',
  `freight_charge` decimal(14,2) NOT NULL DEFAULT '0.00',
  `pay_amount` decimal(14,2) NOT NULL DEFAULT '0.00',
  `pay_order_no` varchar(32) NOT NULL DEFAULT '',
  `address_id` int NOT NULL DEFAULT '0',
  `pay_time` timestamp NULL DEFAULT NULL,
  `updated_time` timestamp NULL DEFAULT NULL,
  `send_out_time` timestamp NULL DEFAULT NULL,
  `finished_time` timestamp NULL DEFAULT NULL,
  `coupon_amount` decimal(14,2) NOT NULL DEFAULT '0.00',
  `order_month` int NOT NULL DEFAULT '0',
  `test1` int DEFAULT '0',
  `test2` int DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=67213 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci)
```