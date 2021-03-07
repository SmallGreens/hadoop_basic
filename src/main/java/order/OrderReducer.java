package order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Matt
 * @date 2021/3/7 13:15
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        for (NullWritable value: values) {
            System.out.println(key.toString());
        }
        context.write(key, NullWritable.get());
    }
}
