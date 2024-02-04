package org.example;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description: 使用Flink 对接 Socket 进行数据处理
 * @date: 1/22/24 11:11
 */
public class FlinkSocketWCApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //那可以预想这个hostname和port是常常要变化的，那如何传入呢而不是写死呢？
        //Flink自己给了一个借口接受hostname和port  -- ParameterTool
        // 在IDEA中，通过编辑运行文件加实参
        // 如果提交Jar来运行，需要排除test来编译
        ParameterTool pT = ParameterTool.fromArgs(args);
        String host = pT.get("host");
        int port = pT.getInt("port");
        DataStreamSource<String> source = executionEnvironment.socketTextStream(host,port);

        source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out)->{
                    String[] splits = value.split(",");
                    for(String split : splits){
                        out.collect(Tuple2.of(split.trim(),1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();
//        new XuFlatMapFunction()

        executionEnvironment.execute(
                "new"
        );

    }
}
