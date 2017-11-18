package nbc.mapreduce

import common.arithmetic.groovy.NaBayesClaMRUtil
import hdfs.HdfsUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

/**
 *********************************************************
 ** @desc ： 朴素贝叶斯分类算法(NBC)
 ** @author Pings
 ** @date 2017/11/17
 ** @version v1.0
 * *******************************************************
 */
class NBC {

    //**测试数据
    static List<MapWritable> testDatas = []
    static String[] propertiesDesc = []

    /**
     *********************************************************
     ** @desc ： 解析hdfs上的测试数据，加载到内存                                            
     ** @author Pings  
     ** @param  file                                   
     ** @date   2017-11-14                                                                                  
     * *******************************************************
     */
    static void parseTestData(String file) {
        FileSystem fs
        InputStream is
        InputStreamReader ir
        BufferedReader br
        try{
            fs = HdfsUtil.getFileSystem()
            is = fs.open(new Path(file))
            ir = new InputStreamReader(is)
            br = new BufferedReader(ir)

            int i = 0
            for(String line = br.readLine(); line != null; i++)
                //**第一行为属性描述
                !i ? (propertiesDesc = line.split(" ")) : testDatas.add(getData(line, "test"))
        } finally {
            br.close()
            ir.close()
            is.close()
            fs.close()
        }
    }

    /**
     *********************************************************
     ** @desc ： 解析数据，转换成可序列化的数据                                          
     ** @author Pings  
     ** @param  line    
     ** @param  type                                
     ** @date   2017-11-17                                                                                  
     * *******************************************************
     */
    static MapWritable getData(String line, type = "train") {
        MapWritable rst = [:]

        String[] datas = line.split(" ")

        //**训练数据最后一列为分类
        if (type == "train")
            rst.put(NaBayesClaMRUtil.CLA, new Text(datas[datas.length - 1]))

        for (int i = 0; i < datas.length; i++) {
            rst.put(new Text(datas[i]), new IntWritable(1))
        }

        return rst
    }

    static class NBCMapper extends Mapper<LongWritable, Text, Text, MapWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            MapWritable trainData = getData(value.toString())

            testDatas.each {
                context.write(it, NaBayesClaMRUtil.getSingleUnionPro(it, trainData))
            }
        }
    }

    static class NBCReducer extends Reducer<Text, MapWritable, Text, Text> {

        protected void reduce(Text key, Iterable<MapWritable> values, Mapper.Context context) throws IOException, InterruptedException {

        }
    }
}
