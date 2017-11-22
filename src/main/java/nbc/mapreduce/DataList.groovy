package nbc.mapreduce

import common.arithmetic.groovy.NaBayesClaMRUtil
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.MapWritable

/**
 *********************************************************
 ** @desc ： map统计后的数据集
 ** @author Pings
 ** @date 2017/11/21
 ** @version v1.0
 * *******************************************************
 */
class DataList {

    //**统计数据
    private Map<String, Map<String, Integer>> datas = [:]
    //**每种分类的数量
    private Map<String, Integer> clas = [:]
    //**数据集总数目
    long total = 0
    //**测试数据的分类
    String testCla

    /**
     *********************************************************
     ** @desc ：保存统计数据，并分类
     ** @author Pings
     ** @date   2017/11/16
     ** @param  trains 所有训练数据
     ** *******************************************************
     */
    void putTrains(MapWritable mapData) {
        def cla = mapData.get(NaBayesClaMRUtil.CLA).toString()

        mapData.each {
            //**训练和测试数据的对比属性，cla(分类)和total(数据总数)排除
            def property = it.getKey()
            if(property == NaBayesClaMRUtil.CLA || property == NaBayesClaMRUtil.TOTAL) return
            property = property.toString()

            //**训练和测试数据的对比数量
            def oldUnionCount = (it.getValue() as IntWritable).get()

            //**每种分类的数量
            clas.containsKey(clas) ? datas.put(cla, clas.get(cla) + 1) : clas.put(cla, 1)

            //**已存在的分类
            if(datas.containsKey(cla)) {
                Map<String, Integer> unionQty = datas.get(cla)
                def newUnionCount = unionQty.containsKey(property) ? unionQty.get(property) + oldUnionCount : oldUnionCount
                unionQty.put(property, newUnionCount)
            } else { //**不存在的分类
                datas.put(cla, [(property): oldUnionCount])
            }

            total += 1
        }
    }

    /**
     *********************************************************
     ** @desc ：获取指定分类的所有训练数据
     ** @author Pings
     ** @date   2017/11/16
     ** @param  rst 数据的结果类型
     ** @return 结果类型为rst的所有训练数据
     ** *******************************************************
     */
    Map<String, Integer> getTrains(String cla) {
        datas.get(cla)
    }

    /**
     *********************************************************
     ** @desc ：获取指定分类的数量
     ** @author Pings
     ** @date   2017/11/21
     ** @param  cla 分类
     ** @return 指定分类的数量
     ** *******************************************************
     */
    int getClaCount(String cla) {
        this.clas.get(cla)
    }

    /**
     *********************************************************
     ** @desc ：获取所有的分类
     ** @author Pings
     ** @date   2017/11/16
     ** @return  所有的结果类型
     ** *******************************************************
     */
    List<String> getAllCla() {
        this.clas.keySet().toArray()
    }

    @Override
    String toString() {
        return "{total=${total}, clas=${clas}, datas=${datas}, testCla=${testCla}"
    }
}
