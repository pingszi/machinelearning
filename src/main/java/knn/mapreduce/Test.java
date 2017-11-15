package knn.mapreduce;

import hdfs.HdfsUtil;
import knn.java.Point;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *********************************************************
 ** @desc  ： 测试KNN,计算红酒品质
 ** @author  Pings
 ** @date    2017/11/14
 ** @version v1.0
 * *******************************************************
 */
public class Test {
    public static String trainFile = "D:\\java\\source\\Pings\\machinelearning\\src\\main\\java\\knn\\data-training.txt";
    public static String testFile = "D:\\java\\source\\Pings\\machinelearning\\src\\main\\java\\knn\\data-test.txt";

    public static String outputDir = "/knn/output/";
    public static String inputTrainFile = "/knn/input/data-training.txt";
    public static String inputTestFile = "/knn/output/data-test.txt";

    /**上传数据,删除输出目录*/
    public static void init() throws Exception {
        HdfsUtil.del(outputDir);
        HdfsUtil.mkdirs("/knn/input/");

        HdfsUtil.upload(trainFile, inputTrainFile);
        HdfsUtil.upload(testFile, inputTestFile);
    }

    public static void main(String[] args) throws Exception {
        //**先在本地执行本方法，初始化数据
        //init();

        //**解析测试数据，加载到内存
        KNN.parseTestData(inputTestFile);

        //**传递r的k值
        Configuration conf = new Configuration();
        conf.set("r", args[0]);
        conf.set("k", args[1]);

        Job job = Job.getInstance(conf);
        job.setJobName("KNN");
        job.setJarByClass(KNN.class);

        job.setMapperClass(KNN.KNNMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Point.Distance.class);
        FileInputFormat.addInputPath(job, new Path(inputTrainFile));

        job.setReducerClass(KNN.KNNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Point.Distance.class);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        job.waitForCompletion(true);
    }
}
