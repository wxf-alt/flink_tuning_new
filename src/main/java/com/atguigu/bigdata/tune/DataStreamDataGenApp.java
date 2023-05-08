package com.atguigu.bigdata.tune;

import com.atguigu.bigdata.tune.bean.UserInfo;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;

/**
 * @Author lzc
 * @Date 2022/10/3 09:24
 */
public class DataStreamDataGenApp {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        
        /*env
            .addSource(new DataGeneratorSource<>(new RandomGenerator<OrderInfo>() {
                @Override
                public OrderInfo next() {
                    return new OrderInfo(
                        random.nextInt(1, 100000),
                        random.nextLong(1, 1000000),
                        random.nextUniform(1, 1000),
                        System.currentTimeMillis()
                    );
                }
            }))
            .returns(OrderInfo.class)
            .print("order");*/
        
        
        env
            .addSource(new DataGeneratorSource<UserInfo>(new SequenceGenerator<UserInfo>(1, 1000000) {
                RandomDataGenerator random = new RandomDataGenerator();
                
                @Override
                public UserInfo next() {
                    return new UserInfo(
                        valuesToEmit.peek().intValue(),
                        valuesToEmit.poll().longValue(),
                        random.nextInt(1, 100),
                        random.nextInt(0, 1)
                    );
                }
            }
            ))
            .returns(UserInfo.class)
            .print("user");
        
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
