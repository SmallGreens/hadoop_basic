import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/3 22:35
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 1. 获取 job 对象 -- 实际 hadoop 中执行的是一个个 job
        Job job = Job.getInstance(conf);

        // 2. 设置 jar 存储位置
        job.setJarByClass(WordCountDriver.class);  // 动态确定 job jar，通过 反射机制实现

        // 3. 关联 map 和 reducer 类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. 设置 mapper 阶段输出数据类型 - k， v 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终的数据输出 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 设置 输入路径，和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));  // org.apache.hadoop.mapreduce.lib.input.注意导包不要导错了
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交 job
        // job.submit();
        boolean result = job.waitForCompletion(true);        // 包装了 submit 方法，添加了打印信息功能
        System.out.println(result);
    }

}
