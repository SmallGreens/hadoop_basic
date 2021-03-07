package tableJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Matt
 * @date 2021/3/7 17:26
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // 存储所有订单集合
        List<TableBean> orderBeans = new ArrayList<>();
        // 存储产品信息
        TableBean pdBean = new TableBean(); // note: 同一个 key 只有一个产品表

        for(TableBean bean: values){
            if("order".equals(bean.getFlag())){
                TableBean tmpBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpBean, bean);    // 实现深拷贝-- bean 是一个引用
                    orderBeans.add(tmpBean);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }else{
                try {
                    BeanUtils.copyProperties(pdBean, bean);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        // 两张表合并
        for (TableBean tableBean: orderBeans) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean, NullWritable.get());
        }

    }
}
