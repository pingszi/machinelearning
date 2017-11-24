package nbc.groovy

import common.arithmetic.groovy.NaBayesClaUtil
import org.apache.log4j.LogManager
import org.apache.log4j.Logger

/**
 *********************************************************
 ** @desc ： 朴素贝叶斯分类算法(NBC)
 ** @author Pings
 ** @date 2017/11/16
 ** @version v1.0
 * *******************************************************
 */
class NBC {

    private static Logger logger = LogManager.getLogger(NBC.class)

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
    static Map<String, Object> getRst(DataList dataList, int coeff = 0) {
        Map<String, Double> rst = [:]

        //**获取测试数据每个分类出现的概率
        def allPro = NaBayesClaUtil.getAllPro(dataList, coeff)

        //**属性和描述
        String propertiesDesc = ""
        def properties = dataList.getTestData().getProperties()
        for (int i = 0; i < properties.size(); i++) {
            propertiesDesc += dataList.getPropertiesDesc().get(i) + ' = ' + properties.get(i) + ", "
        }
        propertiesDesc = propertiesDesc.substring(0, propertiesDesc.length() - 2)

        def claDesc = dataList.getClaDesc()

        //**替换描述信息
        allPro.each {
            def key = "P(${claDesc} = ${it.getKey()} | ${propertiesDesc})"
            rst.put(key, it.getValue())

            logger.info("${key} = ${it.getValue()}")
        }

        return rst
    }
}
