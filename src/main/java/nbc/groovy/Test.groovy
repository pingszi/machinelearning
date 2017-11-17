package nbc.groovy

/**
 *********************************************************
 ** @desc ： 测试朴素贝叶斯分类算法(NBC)
 ** @author Pings
 ** @date 2017/11/16
 ** @version v1.0
 * *******************************************************
 */
class Test {

    /**
     *********************************************************
     ** @desc ：解析文本文件数据
     ** @author Pings
     ** @date   2017/11/11
     ** @param  fileName 文件名称
     ** @param  type (train)训练/(test)测试数据
     ** @return 指定点的结果
     ** *******************************************************
     */
    static Map<String, Object> parseData(fileName, type = "train") {
        def points = []
        def desc = []

        def index = 0
        new File(fileName).eachLine {
            //**第一行为训练数据分类的描述
            if (!index++)
                desc = it.split(" ")
            else
                points.add(getPoint(it, type))
        }

        [data: points, desc: desc]
    }

    /**
     *********************************************************
     ** @desc ：解析每行数据，转换成point对象
     ** @author Pings
     ** @date   2017/11/11
     ** @param  line 每行数据
     ** @param  type (train)训练/(test)测试数据
     ** @return 转换后的点
     ** *******************************************************
     */
    static Point getPoint(line, type = "train") {
        if (line == null || line.trim().length() == 0)
            throw new IllegalArgumentException(line + " is null");

        def datas = line.split(" ")

        if (type == "train")
            new Point(datas[0..datas.size() - 2], datas[datas.size() - 1])
        else
            new Point(datas[0..datas.size() - 1], null)
    }

    static DataList getDataList(List<String> desc, Point testData, List<Point> trains) {
        new DataList(testData, trains, desc.subList(0, desc.size() - 1), desc.get(desc.size() - 1))
    }

    static void main(String[] args) {
        long start = System.currentTimeMillis()

        def test =  parseData("D:\\java\\source\\Pings\\machinelearning\\src\\main\\java\\nbc\\data-test.txt", "test")
        def trains =  parseData("D:\\java\\source\\Pings\\machinelearning\\src\\main\\java\\nbc\\data-training.txt")

        DataList dataList = getDataList((List<String>)trains.get("desc"), test.get("data").get(0), trains.get("data"))
        Map<String, Object> rst = NBC.getRst(dataList)

        long end = System.currentTimeMillis()

        println "计算结果：${dataList.getTestData().getCla()}"
        println "计算时长：${end - start}毫秒"
    }
}
