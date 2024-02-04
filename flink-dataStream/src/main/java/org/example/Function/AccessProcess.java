package org.example.Function;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.example.bean.Access;

/**
 * @version 1.0
 * @author: YuxuanXu
 * @Description:
 * @date: 1/31/24 18:47
 */
public class AccessProcess extends RichFilterFunction<Access> {

    @Override
    public boolean filter(Access value) throws Exception {
        return value.getTraffic() > 4000;
    }
}
