package wordCountCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/3 22:20
 *
 * 统计每个单词出现的次数
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable();  // 同样的，将生成对象的操作写在类中，减少 reduce 中run 函数循环里面频繁重复 创建对象

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        // 1. 累加求和
        for(IntWritable value: values){
            sum += value.get();
        }

        // 2. 写出
        v.set(sum);
        context.write(key, v);

    }
}
