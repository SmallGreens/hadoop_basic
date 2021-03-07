package order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Matt
 * @date 2021/3/7 14:32
 *
 * 该 compare 位于 reduce 阶段(位于 reduce 类之前), 接收的内容为 map 的输出
 *
 * 具体的作用是 对 key 进行重新的排序。
 *
 */
public class OrderGroupingComparator extends WritableComparator {

    protected OrderGroupingComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 要求只要是 id 相同，就认为是相同的 key

        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;

        return Integer.compare(aBean.getOrderId(), bBean.getOrderId());

    }
}
