package myflink;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2019/8/8
 *  @Time: 13:35
 *  
 */

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * CountAgg 实现了AggregateFunction 接口，功能是统计窗口中的条数，即遇到一条数
 * 据就加一。泛型的第一个参数为输入的值的类型，第二个参数为累加器的类型
 * 第三个参数为输出结果的类型
 */

public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    //创建累加器，初始值为0，类型为Long
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    //有新值传入是，累加器加1
    @Override
    public Long add(UserBehavior userBehavior, Long acc) {
        return acc + 1;
    }

    //返回累加之后的结果
    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    //累加器合并
    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}
