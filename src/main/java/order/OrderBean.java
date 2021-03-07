package order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/7 12:52
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;    // 订单 id
    private double price;   // 订单价格

    public OrderBean() {
        super();
    }

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        // 先按照 id 升序排序，再按照 price 降序排序
        int res;

        if(orderId > o.getOrderId()){
            res = 1;
        }else if(orderId < o.getOrderId()){
            res = -1;
        }else{
            // 可以一行搞定！ ：  res = Double.compare(o.getPrice(), price);
            if(price > o.getPrice()){
                res = -1;   // 降序！
            }else if(price < o.getPrice()){
                res = 1;
            }else{
                res = 0;
            }
        }

        return res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);       // 序列化方法：writeInt 和 write 有什么区别？？
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        orderId = dataInput.readInt();
        price = dataInput.readDouble();
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "orderId=" + orderId +
                ", price=" + price;
    }
}
