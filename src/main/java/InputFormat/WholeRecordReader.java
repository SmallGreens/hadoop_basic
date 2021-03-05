package InputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/5 14:49
 *
 * text 中放置文件的路径 + 名称； ByteWritable 放置文件的内容
 *
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    FileSplit split;
    Text k = new Text();
    BytesWritable v = new BytesWritable();  // 注意是 bytes！！而不是 byte
    Configuration conf;
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        // 初始化
        split = (FileSplit) inputSplit;

         conf = context.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(isProgress){
            // 核心业务逻辑
            byte[] buf = new byte[(int) split.getLength()];     // 一次读取切片的所有内容

            // 1. 获取 fs 对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(conf);

            // 2. 获取输入流
            FSDataInputStream fis = fs.open(path);

            // 3. 拷贝 -- 由于 v的设置只提供了 byte[] 写入的接口，所以这里使用一个buf 做数据中转
            IOUtils.readFully(fis, buf, 0, buf.length);     // 文件内容到 buf 缓冲区中

            // 4. 封装 v
            v.set(buf, 0, buf.length);

            // 5. 封装 k
            k.set(path.toString());

            // 6. 关闭资源
            IOUtils.closeStream(fis);

            isProgress = false;     // 文件读取完毕，置为 false
            return true;    // 返回 true，让 mapper 类中处理数据
        }
        return false;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {

        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
