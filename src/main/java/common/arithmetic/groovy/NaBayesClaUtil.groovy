package common.arithmetic.groovy

import nbc.groovy.DataList
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

/**
 *********************************************************
 ** @desc ： 朴素贝叶斯分类算法
 ** @author Pings
 ** @date 2017/11/16
 ** @version v1.0
 * *******************************************************
 */
class NaBayesClaUtil {

    private static Logger logger = LogManager.getLogger(NaBayesClaUtil.class)

    /**
     *********************************************************
     ** @desc ：计算测试数据每个分类出现的概率
     ** @author Pings
     ** @date   2017/11/17
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

            logger.debug("P(x | ${dataList.getClaDesc()} = ${it})P(${dataList.getClaDesc()} = ${it}) = ${pro}")
        }

        logger.debug("P(x) = ${Px}")

        def maxPro = 0
        String testCla
        rst.each {
            //**P(ci|X) = P(ci)P(x|ci) / Px
            def pro = (double)it.getValue() / Px
            it.setValue(pro)

            //**概率最大的分类
            if (maxPro < pro) {
                maxPro = pro
                testCla = it.getKey()
            }
        }

        logger.debug("最大概率的分类：${testCla}, 概率 = ${maxPro}")

        dataList.getTestData().setCla(testCla)

        return rst
    }

    /**
     *********************************************************
     ** @desc ：计算先验概率P(Ci)
     ** @author Pings
     ** @date   2017/11/16
     ** @param  dataList 数据集
     ** @param  cla 分类
     ** @return 先验概率P(Ci)
     ** *******************************************************
     */
    static double getPriorPro(DataList dataList, String cla) {
        def rst = (double) dataList.getTrains(cla).size() / (double) dataList.getTotal()

        logger.debug("先验概率P(${dataList.getClaDesc()} = ${cla}) = ${rst}")

        return rst
    }

    /**
     *********************************************************
     ** @desc ：计算似然度P(X|Ci)
     ** @author Pings
     ** @date   2017/11/16
     ** @param  dataList 数据集
     ** @param  cla      分类
     ** @param  coeff    平滑系数
     ** @return 似然度P(X|Ci)
     ** *******************************************************
     */
    static double getPosteriorPro(DataList dataList, String cla, int coeff = 1) {
        //**属性描述
        def propertiesDesc = dataList.getPropertiesDesc()
        //**似然度
        def posteriorPro = 1

        //**似然度 = 每个属性和指定分类联合概率的乘积
        propertiesDesc.each {
            posteriorPro *= getUnionPro(dataList, cla, it, coeff)
        }

        logger.debug("似然度P(x | ${dataList.getClaDesc()} = ${cla}) = ${posteriorPro}")

        return posteriorPro
    }

    /**
     *********************************************************
     ** @desc ：计算指定属性和指定分类的联合概率P(Xk|Ci)
     ** @author Pings
     ** @date   2017/11/16
     ** @param  dataList 数据集
     ** @param  cla      分类
     ** @param  property 指定属性
     ** @param  coeff    平滑系数
     ** @return 联合概率P(Xk|Ci)
     ** *******************************************************
     */
    static double getUnionPro(DataList dataList, String cla, String property, int coeff = 1) {
        def rst

        //**指定属性所在的列
        def index = dataList.getPropertiesDesc().findIndexOf {it == property}

        //**测试数据属性K的值
        def value = dataList.getTestData().getProperties().get(index)

        //**指定分类的数量
        def trains = dataList.getTrains(cla)

        //**（属性k = 测试数据的值 and 分类为ci）的数量
        def qty = trains.findAll {it.getProperties().get(index) == value}.size()

        //**P(Xk|Ci) =  （属性k = 测试数据的值 and 分类为ci）的数量 / 分类为ci的数量
        if(qty)
            rst =(double) qty / (double) trains.size()
        else
            rst = (double) (qty + coeff) / (double) (trains.size() + coeff) //**0概率值做平滑

        logger.debug("联合概率P(${property} = ${value} | ${dataList.getClaDesc()} = ${cla}) = ${rst}")

        return rst
    }
}
