package wordCountCombiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/3 22:04
 *
 * keyin: 输入数据的 key -- 输入数据的偏移量
 * valuein: 输入数据的 value，text 文本
 * keyout: text 类型- 单词
 * keyvalue: 单词的个数
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);   // 将这两个变量声明为类的属性，而不要每次循环创建新的对象。-- 创建对象操作很消耗资源。
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取文本中的一行数据
        String line = value.toString();

        // 2. 切割单词
        String[] words = line.split(" ");

        // 3. 循环写出
        for(String word: words){
            k.set(word);
            context.write(k, v);
        }
    }
}
