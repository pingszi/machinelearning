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
            rst.put(new Text("${it.getKey()} | ${trainCla}"), trainData.keySet().contains(it.getKey()) ? ONE : ZERO)
        }

        return rst
    }

    /**
     *********************************************************
     ** @desc ：计算测试数据每个分类出现的概率
     ** @author Pings
     ** @date   2017/11/23
     ** @param  dataList 数据集
     ** @param  coeff    平滑系数
     ** @return 每个分类出现的概率
     ** *******************************************************
     */
    static Map<String, Double> getAllPro(DataList dataList, int coeff = 0) {
        //**第一次假设没有0概率问题计算
        getAllPro(dataList,false, coeff)
    }

    /**
     *********************************************************
     ** @desc ：计算测试数据每个分类出现的概率
     ** @author Pings
     ** @date   2017/11/17
     ** @param  dataList 数据集
     ** @param  isCoeff  是否平滑
     ** @param  coeff    平滑系数
     ** @return 每个分类出现的概率
     ** *******************************************************
     */
    protected static Map<String, Double> getAllPro(DataList dataList, boolean isCoeff, int coeff = 0) {
        Map<String, Double> rst = [:]

        //**所有分类
        def classes = dataList.getAllCla()
        logger.debug("所有分类" + classes)

        //**加法法则：P(c1)P(x|c1) / p(x) + P(c2)P(x|c2) / p(x) + ... P(ci)P(x|ci) / p(x) = 1
        //**转换：P(c1)P(x|c1) + P(c2)P(x|c2) + ... + P(ci)P(x|ci) = Px
        def Px = 0
        for(int i = 0; i < classes.size(); i++) {
            def it = classes.get(i)
            //**是否平滑
            def pro = isCoeff ? getPriorPro(dataList, it, coeff) * getPosteriorPro(dataList, it, coeff) : getPriorPro(dataList, it) * getPosteriorPro(dataList, it)
            //**出现0概率问题，强制加入平滑后重新计算 2017-11-23
            if(!pro) {
                logger.debug("P(x | ${it})P(${it}) = ${pro}, 出现0概率问题，加入平滑后重新计算...")
                if(!coeff) throw new RuntimeException("发后0概率问题，需要设置平滑系数...")

                return getAllPro(dataList,true, coeff)
            }

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
     ** @param  coeff   平滑系数
     ** @return 先验概率P(Ci)
     ** *******************************************************
     */
    static double getPriorPro(DataList dataList, String cla, int coeff = 0) {
        coeff ? logger.debug("平滑系数：${coeff}...") : logger.debug("没有平滑...")
        //**先验概率平滑：分子 = 原分子 + 平滑系数，分母 = 原分子 + 训练数据的分类总数 * 平滑系数
        def rst = ((double) dataList.getClaCount(cla) + coeff) / ((double) dataList.getTotal() + coeff * dataList.getAllCla().size())
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
    static double getPosteriorPro(DataList dataList, String cla, int coeff = 0) {
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
    static double getUnionPro(DataList dataList, String cla, Map.Entry<String, Integer> property, int coeff = 0) {
        int qty = property.getValue()

        //**P(Xk|Ci) =  （属性k = 测试数据的值 and 分类为Ci）的数量 / 分类为Ci的数量
        //**P(Xk|Ci)平滑：分子 = 原分子 + 平滑系数，分母 = 原分子 + 指定属性种类数量 * 平滑系数(指定属性种类数量固定设置为1，没有实现)
        def rst = (double) (qty + coeff) / (double) (dataList.getClaCount(cla) + coeff * 1)

        logger.debug("联合概率P(${property.getKey()}) = ${rst}")
        return rst
    }
}
