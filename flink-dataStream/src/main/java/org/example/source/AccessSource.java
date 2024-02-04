package org.example.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.example.bean.Access;

import java.util.Random;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description: 自定义数据源  单并行度也不能改，作用就是造数据
 * @date: 1/30/24 11:51
 */
public class AccessSource implements SourceFunction<Access> {
    boolean isRunning = true;


    @Override
    public void run(SourceContext<Access> sourceContext) throws Exception {
        Random random = new Random();
        String[] domains = {"org.example1","org.example2","org.example3"};
        while (isRunning){
            sourceContext.collect(new Access(System.currentTimeMillis(),domains[random.nextInt(domains.length)],random.nextInt(1000) + 100));

            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {

    }
}
