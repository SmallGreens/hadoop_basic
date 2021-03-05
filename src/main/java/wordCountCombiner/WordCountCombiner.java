package wordCountCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/5 21:49
 *
 * 使用 combiner 实现累加求和
 *
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // 1. 累加求和
        for(IntWritable value: values){
            sum += value.get();
        }

        v.set(sum);

        // 2. 写出
        context.write(key, v);
    }
}
