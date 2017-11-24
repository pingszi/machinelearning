package nbc.mapreduce

import hdfs.HdfsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 *********************************************************
 ** @desc ： 测试朴素贝叶斯分类算法(NBC)
 ** @author Pings
 ** @date 2017/11/17
 ** @version v1.0
 * *******************************************************
 */
class Test {

    static String trainFile = "D:\\java\\source\\Pings\\machinelearning\\src\\main\\java\\nbc\\data-training.txt"
    static String testFile = "D:\\java\\source\\Pings\\machinelearning\\src\\main\\java\\nbc\\data-test.txt"

    static String outputDir = "/nbc/output/"
    static String inputTrainFile = "/nbc/input/data-training.txt"
    static String inputTestFile = "/nbc/input/data-test.txt"

    static String DEFAULT_URL = "hdfs://pings001:9000"

    /**上传数据,删除输出目录*/
    static void init() throws Exception {
        HdfsUtil.del(outputDir)
        HdfsUtil.mkdirs("/nbc/input/")

        HdfsUtil.upload(trainFile, inputTrainFile)
        HdfsUtil.upload(testFile, inputTestFile)
    }

    static void main(String[] args) throws Exception {
        //**先在本地执行本方法，初始化数据
        //init()

        //**解析测试数据，加载到内存
        NBC.parseTestData(inputTestFile)

        //**传递r的k值
        Configuration conf = new Configuration()
        //**设置hdfs的地址
        conf.set("fs.defaultFS", DEFAULT_URL)
        //**设置yarn的地址
        conf.set("yarn.resourcemanager.hostname", "pings001")

        conf.set("c", args[0])

        Job job = Job.getInstance(conf)
        job.setJobName("NBC")
        job.setJarByClass(NBC.class)

        job.setMapperClass(NBC.NBCMapper.class)
        job.setMapOutputKeyClass(Text.class)
        job.setMapOutputValueClass(MapWritable.class)
        FileInputFormat.addInputPath(job, new Path(inputTrainFile))

        job.setReducerClass(NBC.NBCReducer.class)
        job.setOutputKeyClass(Text.class)
        job.setOutputValueClass(NBC.class)
        FileOutputFormat.setOutputPath(job, new Path(outputDir))

        job.waitForCompletion(true)
    }
}
