package com.atguigu.bigdata.tune;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bigdata.source.MockSourceFunction;
import com.atguigu.bigdata.tune.function.NewMidRichMapFunc;
import com.atguigu.bigdata.tune.function.UvRichFilterFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RocksdbTuningApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        EmbeddedRocksDBStateBackend rocksdb = new EmbeddedRocksDBStateBackend();
        rocksdb.setPredefinedOptions(PredefinedOptions.DEFAULT);
        env.setStateBackend(rocksdb);
        
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop162:8020/flink-tuning/ck");
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setTolerableCheckpointFailureNumber(5);
        checkpointConfig.setCheckpointTimeout(60*1000);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        SingleOutputStreamOperator<JSONObject> jsonobjDS = env
                .addSource(new MockSourceFunction())
                .map(JSONObject::parseObject);


        // 按照mid分组，新老用户修正
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = jsonobjDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"))
                .map(new NewMidRichMapFunc());

        // 过滤出 页面数据
        SingleOutputStreamOperator<JSONObject> pageObjDS = jsonWithNewFlagDS.filter(data -> StringUtils.isEmpty(data.getString("start")));

        // 按照mid分组，过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> uvDS = pageObjDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"))
                .filter(new UvRichFilterFunction());

        // 实时统计uv
        uvDS
                .map(r -> Tuple3.of("uv", r.getJSONObject("common").getString("mid"), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .keyBy(r -> r.f0)
                .reduce((value1, value2) -> Tuple3.of("uv", value2.f1, value1.f2 + value2.f2))
                .print().setParallelism(1);


        env.execute();
    }
}
