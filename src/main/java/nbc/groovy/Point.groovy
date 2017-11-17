package nbc.groovy

/**
 *********************************************************
 ** @desc ：点数据
 ** @author Pings
 ** @date 2017/11/16
 ** @version v1.0
 * *******************************************************
 */
class Point {

    Point(List<String> properties, String cla) {
        this.properties = properties
        this.cla = cla
    }

    //**属性
    List<String> properties
    //**分类
    String cla

    @Override
    String toString() {
        return "{properties = " + properties +", cla='" + cla + "}"
    }
}
