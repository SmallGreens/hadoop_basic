package InputFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/5 14:47
 *
 * 例子：练习自定义 InputFormat
 *
 * 需求：将多个小文件合并成一个 sequenceFile 文件（SequenceFile文件是Hadoop用来存储二进制形式的key-value对的文件格式），SequenceFile
 * 里面存储多个文件，存储的形式为 文件路径 + 文件名作为 key， 文件内容作为 value。
 *
 * 具体实现细节：
 * 1. 自定义 WholeFileInputFormat 继承 FileInputFormat, 重写 createRecordReader 方法。
 *
 * 2. 自定义 recordReader, 实现一次读取一个完整的文件封装为 KV 对。
 *  具体的，采用 IO 流一次性读取文件内容作为 value，因为设置了不可切片，最终将所有文件都封装到了 value 中；其次，获取文件路径信息作为 key。
 *
 *  实现的功能实际上类似 小文件归档 har 文件。只是这里生成的是 SequenceFile
 *
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new WholeRecordReader();
    }
}
