package org.example.Function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description:
 * @date: 1/15/24 18:10
 */
public class XuMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception{
        return Tuple2.of(value,1);
    }
}
