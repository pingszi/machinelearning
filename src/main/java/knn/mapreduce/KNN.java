package knn.mapreduce;

import common.arithmetic.DistanceUtil;
import hdfs.HdfsUtil;
import knn.java.Point;
import knn.java.Test;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *********************************************************
 ** @desc  ： KNN算法实现 计算距离
 ** @author  Pings
 ** @date    2017/11/12
 ** @version v1.0
 * *******************************************************
 */
public class KNN {

    //**测试数据
    private static List<Point> testDatas = new ArrayList<>();

    /**
     *********************************************************
     ** @desc ： 解析hdfs上的测试数据，加载到内存                                            
     ** @author Pings  
     ** @param  file                                   
     ** @date   2017-11-14                                                                                  
     * *******************************************************
     */
    public static void parseTestData(String file) {
        String line;

        try(FileSystem fs = HdfsUtil.getFileSystem();
            InputStream in = fs.open(new Path(file));
            InputStreamReader ir = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(ir)) {

            while ((line = br.readLine()) != null) {
                try{
                    testDatas.add(Test.getPoint(line));
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class KNNMapper extends Mapper<LongWritable, Text, Text, Point.Distance> {
        Text k = new Text();
        Point.Distance v = new Point.Distance();

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            //**解析训练数据
            Point p = Test.getPoint(value.toString());

            //**获取R值，默认为1
            String rStr = context.getConfiguration().get("r", "1");
            long r = Long.parseLong(rStr);

            //**遍历测试数据集
            testDatas.forEach(testData -> {
                double distance = DistanceUtil.getDistance(testData.getP(), p.getP(), r);
                v.set(distance, p.getRst());
                k.set(p.toString());

                try {
                    context.write(k, v);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public static class KNNReducer extends Reducer<Text, Point.Distance, Text, Point.Distance> {

        @Override
        protected void reduce(Text key, Iterable<Point.Distance> values, Context context) throws IOException, InterruptedException {
            //**获取K值，默认为1
            String kStr = context.getConfiguration().get("k", "1");
            int k = Integer.parseInt(kStr);

            List<Point.Distance> distances = new ArrayList<>();
            values.forEach(value -> distances.add(value));

            //**获取最近的K个距离
            Collections.sort(distances);
            Point.Distance[] kDs = (Point.Distance[]) distances.subList(0, k).toArray();

            //**计算距离的众数
            Point.Distance rst = k == 1 ? kDs[0] : knn.java.KNN.mode(kDs);

            context.write(key, rst);
        }
    }
}
