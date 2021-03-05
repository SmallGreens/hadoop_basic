package flowSum;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/4 17:11
 */
public class FlowCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"e:/input/phone_data.txt", "e:/output1"};
        // 1. 获取 job 对象
        Job job = Job.getInstance();

        // 2. 设置 jar 的路径
        job.setJarByClass(FlowCountDriver.class);

        // 3. 关联 mapper 和 reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 4. 设置 mapper 输出的 key 和 value 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5. 设置 最终输出的 key 和 value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置分区
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(1);

        // 7、提交 job
        boolean res = job.waitForCompletion(true);

        System.out.println(res);
    }
}
