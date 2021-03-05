package sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/5 20:57
 *
 * 输入：偏移值 + 一行数据；输出值：FlowBean 为 key, 手机号为 Value
 */
public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    Text v = new Text();
    FlowBean k = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // data: 13509468723	7335	110349	117684
        // 1. 获取一行
        String line = value.toString();

        // 2. 切割 -- 赋值给 fields
        String[] fields = line.split("\t");

        // 3. 封装对象
        String phoneNum = fields[0];
        v.set(phoneNum);
        k.setUpFlow(Long.parseLong(fields[1]));
        k.setDownFlow(Long.parseLong(fields[2]));
        k.setSumFlow(Long.parseLong(fields[3]));

        context.write(k, v);


    }


}
