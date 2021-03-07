package outputFormat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Matt
 * @date 2021/3/7 16:10
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        // 实现数据换行
        byte[] tmp = "\r\n".getBytes(StandardCharsets.UTF_8);
        key.append(tmp, 0, tmp.length);

        // 考虑可能有重复的数据，所以循环写入。
        for(NullWritable value: values){
            context.write(key, NullWritable.get());
        }
    }
}
