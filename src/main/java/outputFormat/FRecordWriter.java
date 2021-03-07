package outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Matt
 * @date 2021/3/7 16:16
 */
public class FRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosBaidu;
    FSDataOutputStream fosOther;

    public FRecordWriter(TaskAttemptContext job) {

        try {
            // 1. 获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());

            // 2. 创建输出到 baidu.log 和其他文件夹
            fosBaidu = fs.create(new Path("e:/output1/baidu.log"));
            fosOther = fs.create(new Path("e:/output1/other.log"));

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable nullWritable) throws IOException, InterruptedException {
        // 判断key 中是否有baidu , 进行区分处理
        if(key.toString().contains("baidu")){
            fosBaidu.write(key.toString().getBytes(StandardCharsets.UTF_8));
        }else
            fosOther.write(key.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(fosBaidu);
        IOUtils.closeStream(fosOther);
    }
}
