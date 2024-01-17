package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.Function.XuFlatMapFunction;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description:
 * @date: 1/15/24 21:45
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 运行时的执行模式
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("data/wc.data");
        stringDataStreamSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out)->{
            String[] splits = value.split(",");
            for(String split : splits){
                out.collect(Tuple2.of(split.trim(),1));
            }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)
                .sum(1)
                .print();
//        new XuFlatMapFunction()

        stringDataStreamSource.executeAndCollect(
                "hello World"
        );

    }
}
