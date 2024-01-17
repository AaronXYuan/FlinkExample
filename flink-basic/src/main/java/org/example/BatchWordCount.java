package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.example.Function.XuFlatMapFunction;
import org.example.Function.XuMapFunction;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description: 使用 Flink 进行批处理，并统计 WC
 * @date: 1/14/24 10:10
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.readTextFile("data/wc.data");
        source.flatMap(new XuFlatMapFunction())
                .map(new XuMapFunction())
                .groupBy(0)
                .sum(1)
                .print();

        source.flatMap(new XuFlatMapFunction())
                .map(new XuMapFunction())
                .groupBy(0)
                .sum(1)
                .print();

    }



}
