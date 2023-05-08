package com.atguigu.bigdata.tune;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
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
        conf.setInteger("rest.port", 2000);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setStateBackend(new HashMapStateBackend()); // 默认的状态后端
//        env.setStateBackend(new EmbeddedRocksDBStateBackend()); //
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        env.enableCheckpointing(3000);
//        env.disableOperatorChaining(); // 禁用操作链的优化
        // 禁用优化: taskManager 有 4 个 slot -p 2  -> task: 11 slot:5 taskManger: 2
        // 开启优化: taskManager 有 4 个 slot -p 2  -> task: 9  slot:5 taskManger: 2
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
            .sum(1).setParallelism(5)
            .print();
        
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
