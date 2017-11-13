package knn.mapreduce;

import common.arithmetic.DistanceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *********************************************************
 ** @desc  ： KNN算法实现 计算距离
 ** @author  Pings
 ** @date    2017/11/12
 ** @version v1.0
 * *******************************************************
 */
public class DistanceStep {

    static double[] d4 = {7.4,0.70,0.00,1.9,0.076,11.0,34.0,0.9978,3.51,0.56,9.4};

    public static class DistanceMapper extends Mapper<LongWritable, Text, DoubleWritable, ObjectWritable> {
        DoubleWritable k = new DoubleWritable();
        ObjectWritable v = new ObjectWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] datas = line.split("\t");

            String rStr = context.getConfiguration().get("r");
            if (rStr == null || rStr.trim().length() == 0)
                rStr = "1";
            long r = Long.parseLong(rStr);

            double[] p = new double[datas.length - 2];
            for (int i = 1; i < datas.length - 1; i++) {
                p[i] = Double.parseDouble(datas[i]);
            }

            k.set(DistanceUtil.getDistance(p, d4, r));
            v.set(datas[datas.length - 1]);

            context.write(k, v);
        }
    }

    public static class DistanceReducer extends Reducer<DoubleWritable, ObjectWritable, DoubleWritable, ObjectWritable> {

        int k = 0;
        int current = 0;

        @Override
        protected void reduce(DoubleWritable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            if (k == 0) {
                String kStr = context.getConfiguration().get("k");
                if (kStr == null || kStr.trim().length() == 0)
                    kStr = "1";
                k = Integer.parseInt(kStr);
            }


        }
    }
}
