package order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/7 13:04
 *
 * 注意输出的 value 格式-- nullWritable， 为空。也就是所有的数据都放在了 key 上面
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    // 一行数据：  0000002	Pdt_04	122.4
    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. 获取一行数据
        String line = value.toString();

        // 2. 切割
        String[] fields = line.split("\t");

        // 3. 封装对象
        k.setOrderId(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));

        // 4. 写出
        context.write(k, NullWritable.get());
    }
}
