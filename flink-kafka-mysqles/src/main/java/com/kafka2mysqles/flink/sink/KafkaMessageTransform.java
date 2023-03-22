package com.kafka2mysqles.flink.sink;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageTransform {
    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageTransform.class);

    public  static final JSONObject Message(JSONObject jsonObject){
        //{"before":null,"after":{"id":7621,"order_no":"20230320135701661720","third_order_no":"202303201357019984043","user_id":99356,"inviter_id":10468,"shop_id":2244,"goods_id":31233,"goods_cate_id":312,
        // "goods_name":"goodsName31233","price":31.23,"amount":7,"total_price":218.63,"created_time":"2023-01-08T05:57:01Z","coupon_name":"优惠劵34393","pay_channel":3,"coupon_id":34393,"status":4,
        // "user_remark":"用户备注993568652","freight_charge":30.0,"pay_amount":244.63,"pay_order_no":"P867408491","address_id":129356,"pay_time":"2023-01-08T05:59:50Z","updated_time":"2023-01-08T15:08:52Z",
        // "send_out_time":"2023-01-08T15:08:52Z","finished_time":null,"coupon_amount":4.0,"order_month":0},"source":{"version":"1.6.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1679291821000,
        // "snapshot":"false","db":"test","sequence":null,"table":"tb_order","server_id":1,"gtid":null,"file":"binlog.000014","pos":935609,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1679291821089,
        // "transaction":null}
        JSONObject sourceObject = jsonObject.getJSONObject("source");
        boolean snapshot = sourceObject.getBoolean("snapshot");
        String db = sourceObject.getString("db");
        String table = sourceObject.getString("table");

        JSONObject afterObject = jsonObject.getJSONObject("after");
        String orderNo = afterObject.getString("order_no");

        logger.info("KafkaMessageTransform Message,{},{},{}",db,table,orderNo);
        return jsonObject;
    }
}
