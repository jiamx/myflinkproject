package myflink;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2019/8/8
 *  @Time: 13:50
 *  
 */

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 实现的WindowResultFunction
 * 将主键商品ID，窗口，点击量封装成了ItemViewCount 进行输出。
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    @Override
    public void apply(
            Tuple key,  //窗口主键，即itemid
            TimeWindow window, //窗口
            Iterable<Long> aggregateResult, // 聚合函数的结果，即count的值
            Collector<ItemViewCount> collector //输出类型为ItemViewCount
    ) throws Exception {

        Long itemId = ((Tuple1<Long>) key).f0;
        Long count = aggregateResult.iterator().next();
        collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));


    }
}
