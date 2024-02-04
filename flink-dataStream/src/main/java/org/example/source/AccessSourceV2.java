package org.example.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.example.bean.Access;

import java.util.Random;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description:
 * @date: 1/30/24 18:02
 */
public class AccessSourceV2 implements ParallelSourceFunction {
    boolean isRunning = true;


    @Override
    public void run(SourceContext sourceContext) throws Exception {
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
