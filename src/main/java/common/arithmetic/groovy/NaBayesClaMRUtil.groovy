package common.arithmetic.groovy

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text

/**
 *********************************************************
 ** @desc ： 朴素贝叶斯分类算法(mapreduce)
 ** @author Pings
 ** @date 2017/11/18
 ** @version v1.0
 * *******************************************************
 */
class NaBayesClaMRUtil {

    //**分类
    static final String CLA = new Text("cla")
    //**训练数据总数
    static final String TOTAL = new Text("total")
    //**训练数据中每个分类的的数量
    static final String CLASS_QTY = new Text("classQty")

    /**
     *********************************************************
     ** @desc ：计算每个训练数据与测试数据的联合概率P(Xk|Ci)
     ** @author Pings
     ** @date   2017/11/18
     ** @param  testData  测试数据
     ** @param  trainData 训练数据
     ** @return 每个训练数据与测试数据的联合概率P(Xk|Ci)
     ** *******************************************************
     */
    static MapWritable getSingleUnionPro(MapWritable testData, MapWritable trainData) {
        MapWritable rst = [:]

        //**分类
        def trainCla = trainData.get(CLA)
        rst.put(CLA, new Text(trainCla))
        //**总数量
        rst.put(TOTAL, new IntWritable(1))

        //**联合概率P(Xk|Ci)
        trainData.keySet().retainAll(testData.keySet())
        testData.each {
            rst.put(new Text("${it} | ${trainCla}"), 1)
        }

        return rst
    }

}
