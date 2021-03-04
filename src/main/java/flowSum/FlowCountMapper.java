package flowSum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/4 16:52
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    // map 方法每次读取一行数据，第一个参数 longWritable 为偏移量

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // data 1	13736230513	192.196.100.1	www.baidu.com	2481	24681	200
        // 1. 获取第一行
        String line = value.toString();

        // 2. 切割（\t）
        String[] fields = line.split("\t");

        // 3. 封装对象
        k.set(fields[1]);   // 封装手机号
        long upFlow = Long.parseLong(fields[fields.length -3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);

        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        // 4. 写出
        context.write(k, v);
    }
}
