package flowSum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author Matt
 * @date 2021/3/5 17:31
 *
 * 输入的内容为 map 阶段的输出内容
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int i) {
        // key 是手机号， value 为流量信息

        // 获取手机号的前三位
        String prePhoneNumber = key.toString().substring(0, 3);

        int partition = 4;

        if(prePhoneNumber.equals("136")){
            partition = 0;
        }else if(prePhoneNumber.equals("137")){
            partition = 1;
        }else if(prePhoneNumber.equals("138")){
            partition = 2;
        }else if(prePhoneNumber.equals("139")){
            partition = 3;
        }
        return partition;
    }
}
