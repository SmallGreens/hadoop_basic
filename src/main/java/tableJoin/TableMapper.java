package tableJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/7 17:05
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    TableBean tableBean = new TableBean();
    String name;
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件名称 -- 即切片信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // data: -- 表1
        // id	pid	amount
        //1001	01	1
        //data: 表2
        // pid	pname
        //01	小米

        // 1. 获取一行数据
        String line = value.toString();
        String[] fields = line.split("\t");

        if(name.startsWith("order")){ // 订单表
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setPname("");     // 不能不set，不set 的话会出现序列化异常
            tableBean.setFlag("order");
            k.set(fields[1]);   // 以 pid 为输出的 pid
        }else{
            tableBean.setId("");
            tableBean.setPid(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("pd");
            tableBean.setAmount(0);

            k.set(fields[0]);
        }
        context.write(k, tableBean);



    }
}
