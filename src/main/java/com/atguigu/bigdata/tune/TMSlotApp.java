package com.atguigu.bigdata.tune;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;


/**
 * @Author lzc
 * @Date 2022/10/2 12:24
 */
public class TMSlotApp {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    
        env
            .addSource(new ParallelSourceFunction<String>() {
                @Override
                public void run(SourceContext<String> ctx) throws Exception {
                    Random random = new Random();
                    while (true) {
                        ctx.collect(random.nextInt() + " " + random.nextBoolean());
                        Thread.sleep(100);
                    }
                }
            
                @Override
                public void cancel() {
                
                }
            })
            .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                @Override
                public void flatMap(String value,
                                    Collector<Tuple2<String, Long>> out) throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }
            })
            .keyBy(t -> t.f0)
            .sum(1).setParallelism(4)
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
