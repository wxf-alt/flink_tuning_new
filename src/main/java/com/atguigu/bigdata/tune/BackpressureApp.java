package com.atguigu.bigdata.tune;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;


public class BackpressureApp {
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 禁用算子链的优化. 用来定位问题
        env.disableOperatorChaining();
        
       // env.getCheckpointConfig().enableUnalignedCheckpoints();
        
        env
            .addSource(new SourceFunction<String>() {
                
                volatile boolean flag = true;
                
                @Override
                public void run(SourceContext<String> ctx) throws Exception {
                    while (flag) {
                        ctx.collect("a b c");
                    }
                }
                
                @Override
                public void cancel() {
                    flag = false;
                }
            })
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String line, Collector<String> out) throws Exception {
                    for (String word : line.split(" ")) {
                        
                        
                        word = getString(word);
                        
                        out.collect(word);
                    }
                }
                
                
                private String getString(String word) {
                    for (int i = 0; i < Integer.MAX_VALUE; i++) {
                        word += i;
                    }
                    return word;
                }
            })
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String word) throws Exception {
                    
                    return Tuple2.of(word, 1L);
                }
            })
            .keyBy(r -> r.f0)
            .sum(1)
            .print();
        
        env.execute();
    }
}
