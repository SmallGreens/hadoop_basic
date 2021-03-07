package cache;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Matt
 * @date 2021/3/7 18:17
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    Map<String, String> pdMap = new HashMap<>();
    Text k = new Text();

    // 在 setup 方法中缓存小表
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1. 缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8));

        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            // 切割小表
            String[] fields = line.split("\t");
            pdMap.put(fields[0], fields[1]);        // pid 作为 key
        }

        // 2. 关闭资源
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 获取一行
        String line = value.toString();

        // 2. 切割
        String[] fields = line.split("\t");

        // 3. 获取 pid
        String pid = fields[1];

        // 4. 取出 缓存中的 pname
        String pName = pdMap.get(pid);

        // 5. 拼接
        line = line + "\t" + pName;
        k.set(line);

        context.write(k, NullWritable.get());

    }
}
