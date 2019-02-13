package DTree.java;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *********************************************************
 ** @desc  ： 简单的数据集，单线程版本
 ** @author  Pings
 ** @date    2017/12/8
 ** @version v1.0
 * *******************************************************
 */
public abstract class SimpleAbstractDataList implements DataList {

    //**训练数据集，以分类分组
    protected Map<String, List<Row>> trains = new HashMap<>();
    //**测试数据
    protected Row test;
    //**数据集属性的描述
    protected Desc desc;

    @Override
    public Desc getDesc() {
        return desc;
    }

    @Override
    public Row getTest() {
        return test;
    }
}
