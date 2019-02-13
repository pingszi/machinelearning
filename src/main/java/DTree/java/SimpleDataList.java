package DTree.java;

import java.util.*;

import static java.util.stream.Collectors.*;

/**
 *********************************************************
 ** @desc  ： 简单的数据集，单线程版本
 ** @author  Pings
 ** @date    2017/12/6
 ** @version v1.0
 * *******************************************************
 */
public class SimpleDataList extends SimpleAbstractDataList {

    //**数据集行的总数量
    private long rowQty = 0;

    @Override
    public void putTrain(String[] row) {
        putTrain(new Row(row, false));
    }

    @Override
    public void putTrain(Row row) {
        String c = row.getC();
        if(this.trains.containsKey(c))
            this.trains.get(c).add(row);
        else
            this.trains.put(c, Arrays.asList(row));

        rowQty += 1;
    }

    @Override
    public void setTest(String[] row) {
        this.test = new Row(row, true);
    }

    @Override
    public void setDesc(String[] row) {
        this.desc = new Desc(row);
    }

    @Override
    public Row[] getTrains(String c) {
        List<Row> trains = this.trains.get(c);
        return trains.toArray(new Row[trains.size()]);
    }

    @Override
    public String[] getAllC() {
        Set<String> keys = this.trains.keySet();
        return keys.toArray(new String[keys.size()]);
    }

    @Override
    public long getRowQty() {
        return this.rowQty;
    }

    @Override
    public int getColumnQty() {
        return this.desc.getColumnQty();
    }

    @Override
    public String[] getUniqueColumn(int column) {
        return this.trains.values().stream().flatMap(List::stream).map(row -> row.getProperty(column)).distinct().toArray(String[]::new);
    }

    @Override
    public String[] getColumn(int column) {
        return this.trains.values().stream().flatMap(List::stream).map(row -> row.getProperty(column)).toArray(String[]::new);
    }

    @Override
    public long getColumnQty(int column, String columnValue) {
        return this.trains.values().stream().flatMap(List::stream).filter(row -> row.getProperty(column).equals(columnValue)).count();
    }

    @Override
    public long getColumnQty(int column ,String columnValue, String c) {
        return this.trains.get(c).stream().filter(row -> row.getProperty(column).equals(columnValue)).count();
    }

    @Override
    public Map<String, Long> getColumnAllQty(int column) {
        return this.trains.values().stream().flatMap(List::stream).map(row -> row.getProperty(column)).collect(groupingBy(String::toString, counting()));
    }

    @Override
    public Map<String, Long> getColumnAllQty(int column , String c) {
        return this.trains.get(c).stream().map(row -> row.getProperty(column)).collect(groupingBy(String::toString, counting()));
    }

    @Override
    public long getColumnQty(String c) {
        return this.trains.get(c).size();
    }

    @Override
    public Map<String, Long> getColumnAllQty() {
        return this.trains.entrySet().stream().collect(groupingBy(Map.Entry::getKey, counting()));
    }

}