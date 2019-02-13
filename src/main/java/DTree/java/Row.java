package DTree.java;

import java.util.Arrays;

/**
 *********************************************************
 ** @desc ：行数据
 ** @author Pings
 ** @date 2017/12/06
 ** @version v1.0
 * *******************************************************
 */
public class Row {

    //**属性
    private String[] properties;
    //**分类
    private String c;

    public String getC() {
        return c;
    }

    public void setC(String c) {
        this.c = c;
    }

    public Row(String[] row, boolean isTest) {
        if(isTest) //**测试数据只包含属性
            this.properties = row;
        else {     //**训练数据包含属性和分类
            this.properties = Arrays.copyOf(row, row.length - 1);
            this.c = row[row.length - 1];
        }
    }

    public Row(String[] properties, String c) {
        this.properties = properties;
        this.c = c;
    }

    /**获取指定的属性*/
    public String getProperty(int index) {
        return this.properties[index];
    }

    /**获取所有的属性*/
    public String[] getAllProperty() {
        return properties;
    }

    /**获取列的数量*/
    public int getColumnQty() {
        return this.properties.length;
    }

    @Override
    public String toString() {
        return "Point{properties = " + Arrays.toString(properties) +", c=" + c + "}";
    }
}
