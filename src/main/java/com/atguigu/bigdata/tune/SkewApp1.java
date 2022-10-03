package com.atguigu.bigdata.tune;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bigdata.source.MockSourceFunction;
import com.atguigu.bigdata.tune.function.LocalKeyByFlatMapFunc;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;


public class SkewApp1 {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(TimeUnit.SECONDS.toMillis(3), CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop162:8020/flink-tuning/ck");
        checkpointConfig.setMinPauseBetweenCheckpoints(TimeUnit.SECONDS.toMillis(3));
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        checkpointConfig.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(1));
        

        SingleOutputStreamOperator<JSONObject> jsonobjDS = env
                .addSource(new MockSourceFunction())
                .map(JSONObject::parseObject);

        // 过滤出 页面数据,转换成 (mid,1L)
        SingleOutputStreamOperator<Tuple2<String, Long>> pageMidTuple = jsonobjDS
                .filter(data -> StringUtils.isEmpty(data.getString("start")))
                .map(r -> Tuple2.of(r.getJSONObject("common").getString("mid"), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));


        // 按照mid分组，统计每个mid出现的次数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        boolean isLocalKeyBy = parameterTool.getBoolean("local-keyBy", false);
        if (!isLocalKeyBy) {
            pageMidTuple
                    .keyBy(r -> r.f0)
                    .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                    .print().setParallelism(1);
        } else {
            pageMidTuple
                    .flatMap(new LocalKeyByFlatMapFunc(10000)) // 实现 localkeyby的功能
                    .keyBy(r -> r.f0)
                    .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                    .print().setParallelism(1);
        }

        env.execute();
    }
}
