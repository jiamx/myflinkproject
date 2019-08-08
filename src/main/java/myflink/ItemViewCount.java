package myflink;

/**
 *  @Created with IntelliJ IDEA.
 *  @author : jmx
 *  @Date: 2019/8/8
 *  @Time: 13:26
 *  
 */

/**
 * 商品点击量(窗口操作的输出类型)
 */
public class ItemViewCount {
    public long itemId;    //商品ID
    public long windowEnd; // 窗口结束时间戳
    public long viewCount; // 商品点击量

    public static  ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;

    }
}
