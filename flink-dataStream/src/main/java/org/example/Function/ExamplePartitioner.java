package org.example.Function;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description:
 * @date: 2/1/24 18:17
 */
public class ExamplePartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        System.out.println(numPartitions);
        if("pk1.com".equals(key)) {
            return 0;
        } else if ("pk2.com".equals(key)) {
            return 1;
        }else {
            return 2;
        }
//        return 0;
    }
}
