package com.atguigu.bigdata.tune;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/10/3 09:24
 */
public class SQLDataGenApp {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        
        String orderSql = "CREATE TABLE order_info ( " +
            "    id INT, " +
            "    user_id BIGINT, " +
            "    total_amount DOUBLE, " +
            "    create_time AS localtimestamp, " +
            "    WATERMARK FOR create_time AS create_time " +
            ") WITH ( " +
            "    'connector' = 'datagen', " +
            "    'rows-per-second'='20000', " +
            "    'fields.id.kind'='sequence', " +
            "    'fields.id.start'='1', " +
            "    'fields.id.end'='100000000', " +
            "    'fields.user_id.kind'='random', " +
            "    'fields.user_id.min'='1', " +
            "    'fields.user_id.max'='1000000', " +
            "    'fields.total_amount.kind'='random', " +
            "    'fields.total_amount.min'='1', " +
            "    'fields.total_amount.max'='1000' " +
            ")";
        
        String userSql = "CREATE TABLE user_info ( " +
            "    id INT, " +
            "    user_id BIGINT, " +
            "    age INT, " +
            "    sex INT " +
            ") WITH ( " +
            "    'connector' = 'datagen', " +
            "    'rows-per-second'='20000', " +
            "    'fields.id.kind'='sequence', " +
            "    'fields.id.start'='1', " +
            "    'fields.id.end'='100000000', " +
            "    'fields.user_id.kind'='sequence', " +
            "    'fields.user_id.start'='1', " +
            "    'fields.user_id.end'='1000000', " +
            "    'fields.age.kind'='random', " +
            "    'fields.age.min'='1', " +
            "    'fields.age.max'='100', " +
            "    'fields.sex.kind'='random', " +
            "    'fields.sex.min'='0', " +
            "    'fields.sex.max'='1' " +
            ")";
        
        
        tEnv.executeSql(orderSql);
        tEnv.executeSql(userSql);
        
        tEnv.executeSql("select * from order_info").print();
        //        tableEnv.executeSql("select * from user_info").print();
    }
}
