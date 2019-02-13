package DTree.java;

import java.util.Arrays;

/**
 *********************************************************
 ** @desc  ： 数据集属性的描述
 ** @author  Pings
 ** @date    2017/12/6
 ** @version v1.0
 * *******************************************************
 */
public class Desc {

    //**属性描述
    private String[] properties;
    //**分类描述
    private String c;

    public String getC() {
        return c;
    }

    public Desc(String[] row) {
        this(Arrays.copyOf(row, row.length - 1), row[row.length - 1]);
    }

    public Desc(String[] properties, String c) {
        this.properties = properties;
        this.c = c;
    }

    /**获取指定的属性描述*/
    public String getDesc(int index) {
        return this.properties[index];
    }

    /**获取所有的属性描述*/
    public String[] getAllDesc() {
        return properties;
    }

    /**获取列的数量*/
    public int getColumnQty() {
        return this.properties.length;
    }

    @Override
    public String toString() {
        return "Desc{properties=" + Arrays.toString(properties) + ", c=" + c + "}";
    }
}
