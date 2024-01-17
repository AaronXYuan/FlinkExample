package org.example.Function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description:
 * @date: 1/15/24 18:04
 */
public class XuFlatMapFunction implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception{
        String[] splits = value.split(",");
        for(String split: splits){
            out.collect(split.toLowerCase().trim());
        }
    }
}
