package compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

/**
 * @author Matt
 * @date 2021/3/8 21:01
 *
 * 测试 hadoop 的压缩与解压缩功能。
 *
 * 如果希望在 map 和 reduce 之间添加压缩环节，方法是在 driver 类中添加配置信息：
 *
 * 	// 开启map端输出压缩
 * 	configuration.setBoolean("mapreduce.map.output.compress", true);
 * 	// 设置map端输出压缩方式
 * 	configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
 *
 * 	如果希望在 reduce 输出的为压缩了的文件。进行下列设置即可:
 *
 * 	// 设置reduce端输出压缩开启
 * 	FileOutputFormat.setCompressOutput(job, true);
 *  // 设置压缩的方式
 * 	FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
 *
 */
public class TestCompress {

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 压缩
        // 使用反射方式获取压缩方式
        compress("e:/input/compress/hello.txt", "org.apache.hadoop.io.compress.BZip2Codec");    // 使用 bzip2 方式压缩

        decompress("e:/input/compress/hello.txt.bz2");
    }

    private static void decompress(String fileName) throws IOException {
        // 1. 压缩方式检查
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        if(codec == null){
            System.out.println("can't get the compression style");
            return;
        }

        // 2. 获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        CompressionInputStream cis = codec.createInputStream(fis);

        // 3. 获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + ".decode"));

        // 4. 流的对拷
        IOUtils.copyBytes(cis, fos, 1024*1024, false);

        // 5. 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }

    private static void compress(String fileName, String method) throws IOException, ClassNotFoundException {
        // 1. 获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));

        // 获取压缩方式相关信息
        Class classCodec = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());

        // 2. 获取输出流 --
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        // 3. 流的对拷
        IOUtils.copyBytes(fis, cos, 1024*1024, false);  // 设置的缓冲区为 1M

        // 4. 关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fis);
    }
}
