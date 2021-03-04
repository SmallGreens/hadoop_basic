package flowSum;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/4 17:03
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    FlowBean v = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long sum_upFlow = 0;
        long sum_downFlow = 0;

        // step1: 累加求和
        for(FlowBean val: values){
            sum_upFlow = val.getUpFlow();
            sum_downFlow = val.getDownFlow();
        }
        v.set(sum_upFlow, sum_downFlow);

        // step2: 写出
        context.write(key, v);
    }
}
