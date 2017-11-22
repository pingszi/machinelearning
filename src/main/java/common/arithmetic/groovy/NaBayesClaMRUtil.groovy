package common.arithmetic.groovy

import nbc.mapreduce.DataList
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

/**
 *********************************************************
 ** @desc ： 朴素贝叶斯分类算法(map reduce)
 ** @author Pings
 ** @date 2017/11/18
 ** @version v1.0
 * *******************************************************
 */
class NaBayesClaMRUtil {

    private static Logger logger = LogManager.getLogger(NaBayesClaMRUtil.class)

    //**分类
    static final Text CLA = new Text("cla")
    //**训练数据总数
    static final Text TOTAL = new Text("total")
    //**map reduce可序列化的0
    static final IntWritable ZERO = new IntWritable(0)
    //**map reduce可序列化的1
    static final IntWritable ONE = new IntWritable(1)

    /**
     *********************************************************
     ** @desc ：计算每个训练数据与测试数据的联合数量SUM(Xk|Ci)
     ** @author Pings
     ** @date   2017/11/18
     ** @param  testData  测试数据
     ** @param  trainData 训练数据
     ** @return 每个训练数据与测试数据的联合数量SUM(Xk|Ci)
     ** *******************************************************
     */
    static MapWritable getSingleUnionQty(MapWritable testData, MapWritable trainData) {
        MapWritable rst = [:]

        //**分类
        def trainCla = trainData.get(CLA)
        rst.put(CLA, new Text(trainCla))
        //**总数量
        rst.put(TOTAL, ONE)

        //**联合数量SUM(Xk|Ci)
        trainData.keySet().retainAll(testData.keySet())
        testData.each {
            rst.put(new Text("${it} | ${trainCla}"), trainData.containsKey(it) ? ONE : ZERO)
        }

        return rst
    }

    /**
     *********************************************************
     ** @desc ：计算测试数据每个分类出现的概率
     ** @author Pings
     ** @date   2017/11/21
     ** @param  dataList 数据集
     ** @param  coeff    平滑系数
     ** @return 每个分类出现的概率
     ** *******************************************************
     */
    static Map<String, Double> getAllPro(DataList dataList, int coeff = 1) {
        Map<String, Double> rst = [:]

        //**所有分类
        def classes = dataList.getAllCla()
        logger.debug("所有分类" + classes)

        //**加法法则：P(c1)P(x|c1) / p(x) + P(c2)P(x|c2) / p(x) + ... P(ci)P(x|ci) / p(x) = 1
        //**转换：P(c1)P(x|c1) + P(c2)P(x|c2) + ... + P(ci)P(x|ci) = Px
        def Px = 0
        classes.each {
            def pro = getPriorPro(dataList, it) * getPosteriorPro(dataList, it, coeff)
            rst.put(it, pro)

            Px += pro

            logger.debug("P(x | ${it})P(${it}) = ${pro}")
        }

        logger.debug("P(x) = ${Px}")

        def maxPro = 0
        String testCla
        rst.each {
            //**P(Ci|X) = P(Ci)P(X|Ci) / Px
            def pro = (double)it.getValue() / Px
            it.setValue(pro)

            //**概率最大的分类
            if (maxPro < pro) {
                maxPro = pro
                testCla = it.getKey()
            }
        }

        logger.debug("最大概率的分类：${testCla}, 概率 = ${maxPro}")

        dataList.setTestCla(testCla)

        return rst
    }


    /**
     *********************************************************
     ** @desc ：计算先验概率P(Ci)
     ** @author Pings
     ** @date   2017/11/21
     ** @param  dataList 数据集
     ** @param  cla 分类
     ** @return 先验概率P(Ci)
     ** *******************************************************
     */
    static double getPriorPro(DataList dataList, String cla) {
        def rst = (double) dataList.getClaCount(cla) / (double) dataList.getTotal()

        logger.debug("先验概率P(${cla}) = ${rst}")

        return rst
    }


    /**
     *********************************************************
     ** @desc ：计算似然度P(x|Ci)
     ** @author Pings
     ** @date   2017/11/21
     ** @param  dataList 数据集
     ** @param  cla      分类
     ** @param  coeff    平滑系数
     ** @return 似然度P(X|Ci)
     ** *******************************************************
     */
    static double getPosteriorPro(DataList dataList, String cla, int coeff = 1) {
        //**似然度
        def posteriorPro = 1

        //**似然度 = 每个属性和指定分类联合概率的乘积
        dataList.getTrains(cla).each {
            posteriorPro *= getUnionPro(dataList, cla, it, coeff)
        }

        logger.debug("似然度P(x | ${cla}) = ${posteriorPro}")

        return posteriorPro
    }

    /**
     *********************************************************
     ** @desc ：计算指定属性和指定分类的联合概率P(Xk|Ci)
     ** @author Pings
     ** @date   2017/11/21
     ** @param  dataList 数据集
     ** @param  cla      分类
     ** @param  property 指定属性
     ** @param  coeff    平滑系数
     ** @return 联合概率P(Xk|Ci)
     ** *******************************************************
     */
    static double getUnionPro(DataList dataList, String cla, Map.Entry<String, Integer> property, int coeff = 1) {
        def rst

        int qty = property.getValue()

        //**P(Xk|Ci) =  （属性k = 测试数据的值 and 分类为Ci）的数量 / 分类为Ci的数量
        if(qty)
            rst =(double) qty / (double) dataList.getClaCount(cla)
        else
            rst = (double) (qty + coeff) / (double) (dataList.getClaCount(cla) + coeff) //**0概率值做平滑

        logger.debug("联合概率P(${property.getKey()} | ${cla}) = ${rst}")

        return rst
    }
}
