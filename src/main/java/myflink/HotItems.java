package myflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;
import java.net.URL;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2019/8/8
 *  @Time: 11:25
 *  
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        //创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // UserBehavior.csv的本地路径
        URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
        Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
        //抽取UserBehavior的TypeInformation，是一个Pojotypeinfo
        PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        String[] fieldOrder = {"userId", "itemId", "categoryId", "behavior", "timestamp"};
        //创建PojoCsvInputFormat
        PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);
        DataStreamSource<UserBehavior> dataSource = env.createInput(csvInput, pojoType);
        //Flink默认使用的是ProcessingTime，显示设置为EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        /*
         **第二件事情是指定如何获得业务时间，以及生成 Watermark。Watermark 是用来追踪业务事件
         **的概念，可以理解成 EventTime 世界中的时钟，用来指示当前处理到什么时刻的数据了。由于
         **我们的数据源的数据已经经过整理，没有乱序，即事件的时间戳是单调递增的，所以可以将每
         **条数据的业务时间就当做 Watermark。这里我们用AscendingTimestampExtractor 来实现时间
         * 戳的抽取和 Watermark 的生成
         * */

        DataStream<UserBehavior> timeData = dataSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior userBehavior) {
                //原始数据单位秒，将其转成毫秒
                return userBehavior.timestamp * 1000;
            }
        });
        //  过滤出点击事件
        DataStream<UserBehavior> pvData = timeData.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                // 过滤出点击事件
                return userBehavior.behavior.equals("pv");
            }
        });
        //使用窗口统计点击量

        /* 使用 .aggregate(AggregateFunction af, WindowFunction wf) 做增量的聚合操作，它能使用
         *AggregateFunction 提前聚合掉数据，减少 state 的存储压力。较之.apply(WindowFunction wf)
         *会将窗口中的数据都存储下来，最后一起计算要高效地多
         * 第二个参数WindowFunction 将每个 key 每个窗口聚合后的结果带上其他信息进行输出
         * 返回每个商品在每个窗口的点击量的数据流。
         * */
        DataStream<ItemViewCount> windowedData = pvData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());

        /**
         * 计算热门商品
         * 为了统计每个窗口下最热门的商品，我们需要再次按窗口进行分组，这里根据ItemViewCount
         * 中的windowEnd 进行keyBy()操作。然后使用 ProcessFunction 实现一个自定义的 TopN 函数
         * TopNHotItems 来计算点击量排名前3 名的商品，并将排名结果格式化成字符串，便于后续输出。
         */
        DataStream<String> topItems = windowedData
                .keyBy("windowEnd")
                .process(new TopNHotItem(3));//求点击量在前三名的商品



// 设置执行的并行度
        env.setParallelism(1);
        topItems.print();
        env.execute("Hot Items Job");
    }
}
