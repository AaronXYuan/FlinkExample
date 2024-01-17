package org.example;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.example.function.XuFlatMapFunction;
import org.example.function.XuMapFunction;

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
