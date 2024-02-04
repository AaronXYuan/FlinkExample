package org.example.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description:
 * @date: 1/30/24 18:01
 */
public class AccessSourceParallel extends RichParallelSourceFunction {

    @Override
    public void run(SourceContext sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
