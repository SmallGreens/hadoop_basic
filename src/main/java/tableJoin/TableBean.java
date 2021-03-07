package tableJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/7 16:57
 *
 * 要点1：设计 bean 对象， 需要使用 join 中的关联 key 作为 key。 map 之后给到 reduce 的内容就为根据 前面 key 排序过的内容
 * 要点2： 在 map 中需要回去数据来源，通过 setup 方法来获取。
 */
public class TableBean implements Writable {
    // 1001	01	1
    private String id;  // 订单id
    private String pid;     // 产品 id
    private int amount;     // 产品数量
    private String pname;
    private String flag;    // 标记，标记是产品表还是订单表

    public TableBean() {
    }

    public TableBean(String id, String pid, int amount, String flag, String pname) {
        this.id = id;
        this.pid = pid;
        this.amount = amount;
        this.flag = flag;
        this.pname = pname;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // 序列化
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 反序列化
        id = dataInput.readUTF();
        pid = dataInput.readUTF();
        amount = dataInput.readInt();
        pname = dataInput.readUTF();
        flag = dataInput.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getFlag() {
        return flag;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return id + "\t" + amount + "\t" + pname;
    }
}
