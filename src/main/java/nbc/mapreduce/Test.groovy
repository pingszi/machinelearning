package nbc.mapreduce

import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text

/**
 *********************************************************
 ** @desc ： 测试朴素贝叶斯分类算法(NBC)
 ** @author Pings
 ** @date 2017/11/17
 ** @version v1.0
 * *******************************************************
 */
class Test {

    static void main(String[] args) {
        MapWritable test1 = [:]
        test1.put(new Text("key1"), new Text("value1"))
        test1.put(new Text("key2"), new Text("value2"))
        test1.put(new Text("key3"), new Text("value3"))

        MapWritable test2 = [:]
        test2.put(new Text("key1"), new Text("value1"))
        test2.put(new Text("key3"), new Text("value3"))
        test2.put(new Text("key4"), new Text("value4"))

        test1.keySet().retainAll(test2.keySet())
        println(test1)

        def key5 = "key5s"
        Map<String, Integer> test3 = [(key5): 1]
        println test3.get(key5)
    }
}
