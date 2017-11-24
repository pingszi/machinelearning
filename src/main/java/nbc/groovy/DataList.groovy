package nbc.groovy

/**
 *********************************************************
 ** @desc ： 数据集
 ** @author Pings
 ** @date 2017/11/16
 ** @version v1.0
 **
 ** @update v1.1 增加propertiesCla，用于拉普拉斯平滑
 * *******************************************************
 */
class DataList {

    //**训练数据
    private Map<String, List<Point>> datas = [:]
    //**测试数据
    Point testData
    //**数据集总数目
    long total = 0
    //**数据属性的描述
    List<String> propertiesDesc
    //**数据属性的种类数量，用于拉普拉斯平滑
    List<Integer> propertiesCla
    //**数据分类的描述
    String claDesc

    DataList() {}

    DataList(Point testData, List<Point> trains, List<String> propertiesDesc, List<Integer> propertiesCla, String claDesc = "分类") {
        this.testData = testData
        this.putTrain(trains)
        this.propertiesDesc = propertiesDesc
        this.propertiesCla = propertiesCla
        this.claDesc = claDesc
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
    List<Point> getTrains(String cla) {
        datas.get(cla)
    }

    /**
     *********************************************************
     ** @desc ：保存所有训练数据，并分类
     ** @author Pings
     ** @date   2017/11/16
     ** @param  trains 所有训练数据
     ** *******************************************************
     */
    void putTrain(List<Point> trains) {
        trains.each {
            def cla = it.cla
            datas.containsKey(cla) ? datas.get(cla).add(it) : datas.put(cla, [it])

            total += 1
        }
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
        this.datas.keySet().toArray()
    }

    @Override
    String toString() {
        return "{propertiesDesc=" + propertiesDesc + ", claDesc= " + claDesc + ", datas=" + datas + ", testData=" + testData + '}'
    }
}
