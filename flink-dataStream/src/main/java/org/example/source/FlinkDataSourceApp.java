package org.example.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.bean.Access;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description: Flink中DataSource的使用
 * @date: 1/29/24 18:31
 */
public class FlinkDataSourceApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // fromElements来自于SourceFunction，并行度只能是1，不能改大
//        DataStreamSource<Integer> source = env.fromElements(1,2,3,4,5,6);
        // 最大并行度取决于有几个 core
//        DataStreamSource<Long> source1 = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.TYPE);

        DataStreamSource<Access> source = env.addSource( new AccessSourceV2());

        System.out.println(source.getParallelism());
//        System.out.println(source1.getParallelism());

//        SingleOutputStreamOperator<Integer> mapStream = source.map(x -> x * 10);

//        SingleOutputStreamOperator<Long> map = source1.map(x -> x * 20).setParallelism(3);

//        System.out.println(mapStream.getParallelism());
//        System.out.println(map.getParallelism());
//        map.print();
//        mapStream.print();
        env.execute("new DataSource Example");
    }
}
