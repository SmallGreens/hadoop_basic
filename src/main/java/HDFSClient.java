import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author Matt
 * @date 2021/3/2 16:17
 */
public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();

        // 如果像下面这么写，需要在 ide 中配置 jvm参数，idea 中：help—>Edit Custom VM Options
        // 添加参数：-DHADOOP_USER_NAME=lab1
        // conf.set("fs.defaultFS", "hdfs://slave1:9000");  // 访问 namenode 所在的服务器
        // FileSystem fs = FileSystem.get(conf);

        // 1. 获取一个 hdfs 客户端对象
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1"); // nameNode

        // 2. 在 hdfs 上创建路径
        fs.mkdirs(new Path("/test/xiaoXin"));

        // 3. 关闭资源
        fs.close();

        System.out.println("finish");
    }

    // 1. 文件上传
    // 代码中配置 conf 的优先级 > 代码资源目录下配置文件 > 服务器中配置文件 > hadoop 默认设置值
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取 fs 对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 执行上传 API
        fs.copyFromLocalFile(new Path("e:/test.txt"), new Path("/test/xiaoXin/test.txt"));

        // 3. 关闭资源
        fs.close();

    }

    // 2. 文件下载
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取 fs 对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 执行下载操作
        // fs.copyToLocalFile(new Path("/test/xiaoXin/test.txt"), new Path("e:/test.txt"));
        fs.copyToLocalFile(false, new Path("/test/xiaoXin/test.txt"), new Path("e:/test.txt"), true);

        // 3. 关闭资源
        fs.close();
    }

    // 3. 文件删除
    @Test
    public void testDel() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取 fs 对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 删除文件
        fs.delete(new Path("/test/xiaoXin/test.txt"), false);  // 第二个参数指定是否需要递归删除

        // 3. 关闭资源
        fs.close();
    }

    // 4. 文件更名
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取 fs 对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 更名操作
        fs.rename(new Path("/test/xiaoXin/test.txt"), new Path("/test/xiaoXin/test1.txt"));

        // 3. 关闭资源
        fs.close();

    }

    // 5. hdfs 文件详情的查看
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取 fs 对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);  // true 表示递归查看

        while (listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());  // 文件名
            System.out.println(fileStatus.getPermission());  // 文件权限
            System.out.println(fileStatus.getLen());        // 文件长度
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();    // 获取所有 副本位置
            for(BlockLocation blockLocation: blockLocations){
                System.out.println(Arrays.toString(blockLocation.getHosts()));
            }
            System.out.println("-----------break line----------------");
        }

        // 3. 关闭资源
        fs.close();
    }

    // 6. 判断一个文件是文件还是文件夹
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取 fs 对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 判断操作
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for(FileStatus fileStatus: listStatus){
            if(fileStatus.isFile()){ // 文件
                System.out.println("f: " + fileStatus.getPath().getName());
            }else{  // 文件夹
                System.out.println("d: " + fileStatus.getPath().getName());
            }
        }

        // 3. 关闭资源
        fs.close();
    }


    /**
     * 7- hdfs 的 I/O 流操作
     *
     * 自己实现 上述的 API 操作 -- 使用 IO 流的方式，自定义数据上传与下载
     *
     * 目标：把 e 盘根目录下 test2.txt 文件上传到 hdfs 系统根目录。
     *
     */
    @Test
    public void putFileToHDFS() throws IOException, URISyntaxException, InterruptedException {

        // 1. 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 获取输入流
        FileInputStream fis = new FileInputStream(new File("e:/test2.txt"));

        // 3. 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/test/xiaoHua/test2.txt"));

        // 4. 流的对拷 -- org.apache.hadoop.io.IOUtils
        IOUtils.copyBytes(fis, fos, conf);

        // 5. 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * 8- hdfs 文件的下载
     *
     * 同样使用文件流来实现
     */
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {

        // 1. 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 获取 hdfs 上的输入流
        FSDataInputStream fis = fs.open(new Path("/test/xiaoHua/test2.txt"));

        // 3. 获取到本地的输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/he.txt"));

        // 4. 流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 5. 资源关闭
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    /**
     * HDFS 定位读取文件 -- 下载 多块文件的一部分。 e.g. log 文件只要下载一部分
     *
     */
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.8.3.tar.gz"));

        // 3. 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.8.3.tar.gz.part1"));

        // 4. 流的对拷 -- 只拷贝128 M
        byte[] buf = new byte[1024];    // 设置 1k 缓冲区
        for(int i = 0; i < 1024 * 128; i++){
            fis.read(buf);
            fos.write(buf);
        }

        // 5. 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // 下载第二块
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://slave1:9000"), conf, "lab1");

        // 2. 获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.8.3.tar.gz"));

        // 3. 设置指定读取的起点
        fis.seek(1024*1024*128);

        // 4. 获取输出流
        FileOutputStream fos = new FileOutputStream("e:/hadoop-2.8.3.tar.gz.part2");

        // 5. 流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 6. 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    // windows 下文件拼接： cmd 中执行：`type hadoop-2.8.3.tar.gz.part2 >> hadoop-2.8.3.tar.gz.part1`

}
