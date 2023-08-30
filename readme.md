
# Flink Data CDC
将mysql中的表数据变化同步进kafka
## 安装
1,打包
```shell
mvn clean package
```
2,将meflink-common-1.0-SNAPSHOT.jar上传到flink安装目录的lib目录中    
3,将meflink-mysql-kafka-1.0-SNAPSHOT.jar 通过Flink Dashboard,Submit New Job提交  
4,将resources目录下的mysql2kafka.yaml复制到/etc/flink/conf.d/目录中,配置好mysql与kafka信息  
5,在Flink Dashboard运行Job  

## 测试表

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
  `change_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据库变化时间统一时区',
  `create_date` varchar(20) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`user_id`)
) ENGINE=InnoDB;
```
