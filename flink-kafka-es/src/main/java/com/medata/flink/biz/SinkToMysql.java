package com.medata.flink.biz;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SinkToMysql extends RichSinkFunction<List<JSONObject>> {
    private static final Logger logger = LoggerFactory.getLogger(SinkToMysql.class);
    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<JSONObject> value, Context context) throws Exception {
        //遍历数据集合
        logger.info("SinkToMysql......{}",value.size());
    }
}