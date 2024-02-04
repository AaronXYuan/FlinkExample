package org.example.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.Function.ExamplePartitioner;
import org.example.bean.Access;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description: map:每个元素都作用上同一个方法，得到一个新的 dataStream
 * @date: 1/31/24 17:20
 */
public class TransformationApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 读取 access.log的数据，返回 Access 对象
        DataStreamSource<String> source = env.readTextFile("data/access.log");
//        SingleOutputStreamOperator<Access> map = source.map(new MapFunction<String, Access>() {
//            @Override
//            public Access map(String s) throws Exception {
//                String[] splits = s.split(",");
//                return new Access(Long.parseLong(splits[0]), splits[1], Double.parseDouble(splits[2]));
//            }
//        }).filter(new AccessProcess());
        /**
         * @Description: FlatMap 1对1 1对多
         * @param: args
         * @return void
         * @author YuxuanXu
         * @date 1/31/24 19:07
        */

//        SingleOutputStreamOperator<String> map = source.map(new MapFunction<String, Access>() {
//            @Override
//            public Access map(String s) throws Exception {
//                String[] splits = s.split(",");
//                return new Access(Long.parseLong(splits[0]), splits[1], Double.parseDouble(splits[2]));
//            }
//        }).flatMap(new FlatMapFunction<Access, String>() {
//            @Override
//            public void flatMap(Access access, Collector<String> collector) throws Exception {
//                if (access.getDomain().equals("pk1.com")){
//                    collector.collect(access.getDomain());
//                }else{
//                    collector.collect("--domain--"+access.getDomain());
//                    collector.collect("--domain--"+access.getDomain());
//                }
//            }
//        });

        // KeyBy -- Reduce
//         source.map(new MapFunction<String, Access>() {
//            @Override
//            public Access map(String s) throws Exception {
//                String[] splits = s.split(",");
//                return new Access(Long.parseLong(splits[0]), splits[1], Double.parseDouble(splits[2]));
//            }
//        }).keyBy(new KeySelector<Access, String>() {
//             @Override
//             public String getKey(Access access) throws Exception {
//                 return access.getDomain();
//             }
//         }).sum("traffic")
//                 .print();
//
//         // Union 合并 DataStream
//        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5);
//        DataStreamSource<String> source1 = env.fromElements("1","2","3","4");
//        DataStreamSource<Integer> integerDataStreamSource1 = env.fromElements(2, 8, 6, 8, 6);
//        integerDataStreamSource.union(integerDataStreamSource1);
//
//        // Connect：数据类型可以不同
//        ConnectedStreams<Integer, String> connect = integerDataStreamSource.connect(source1);
//
//        connect.map(new CoMapFunction<Integer, String, String>() {
//
//            @Override
//            public String map1(Integer integer) throws Exception {
//                return ""+integer * 9;
//            }
//
//            @Override
//            public String map2(String s) throws Exception {
//                return "Prefix" + s;
//            }
//        }).print();

        // 分区

//        map.print();
        env.setParallelism(3);
//        source.map(new MapFunction<String, Access>() {
//            @Override
//            public Access map(String s) throws Exception {
//                String[] splits = s.split(",");
//                return new Access(Long.parseLong(splits[0]), splits[1], Double.parseDouble(splits[2]));
//            }
//        }).partitionCustom(new ExamplePartitioner(), (KeySelector<Access, String>) Access::getDomain).process(new ProcessFunction<Access, String>() {
//            @Override
//            public void processElement(Access access, Context context, Collector<String> collector) throws Exception {
//                int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
//                // 收集带有分区信息的数据
//                collector.collect("Subtask Index: " + subtaskIndex + ", Data: " + access.toString());
//            }
//        }).print();

        // 分流操作
        // split 已经被淘汰了用的是
//        source.map(new MapFunction<String, Access>() {
//            @Override
//            public Access map(String s) throws Exception {
//                String[] splits = s.split(",");
//                return new Access(Long.parseLong(splits[0]), splits[1], Double.parseDouble(splits[2]));
//            }
//        }).filter(x -> "pk1.com".equals(x.getDomain()));
        SingleOutputStreamOperator<Access> map = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String s) throws Exception {
                String[] splits = s.split(",");
                return new Access(Long.parseLong(splits[0]), splits[1], Double.parseDouble(splits[2]));
            }
        });
        OutputTag<Access> outputTag1 = new OutputTag<Access>("pk1"){};
        OutputTag<Access> outputTag2 = new OutputTag<Access>("pk2"){};
        SingleOutputStreamOperator<Access> process = map.process(new ProcessFunction<Access, Access>() {
            @Override
            public void processElement(Access access, Context context, Collector<Access> collector) throws Exception {
                if ("pk1.com".equals(access.getDomain())) {
                    context.output(outputTag1, access);
                } else if ("pk2.com".equals(access.getDomain())) {
                    context.output(outputTag2, access);
                } else {
                    collector.collect(access);
                }
            }
        });
        process.getSideOutput(outputTag1).print("PK1:");
        process.getSideOutput(outputTag2).print("PK2:");
        process.print();
        env.execute("Map使用");
    }

}
