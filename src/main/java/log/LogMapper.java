package log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/7 18:54
 *
 * 数据清理 案例
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 获取一行数据
        String line = value.toString();

        // 2. 解析数据
        boolean res = parseLog(line, context);      // 将函数封装起来，程序的架构更加清晰！！

        if(!res) return;

        // 3. 如果数据符合要求，则将数据写出
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");
        if(fields.length <= 11){
            context.getCounter("map", "false").increment(1);
            return false;
        }
        context.getCounter("map", "true").increment(1);     // 调用计数器，统计 true 的个数 -- 即有效数据的个数
        return true;
    }
}
