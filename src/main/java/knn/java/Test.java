package knn.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *********************************************************
 ** @desc  ： 测试KNN,计算红酒品质
 ** @author  Pings
 ** @date    2017/11/11
 ** @version v1.0
 * *******************************************************
 */
public class Test {

    /**
     *********************************************************
     ** @desc ：解析文本文件数据
     ** @author Pings
     ** @date   2017/11/11
     ** @param  fileName 文件名称
     ** @return 指定点的结果
     ** *******************************************************
     */
    public static List<Point> parseData(String fileName) {
        List<Point> points = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))){
            String line;

            while ((line = br.readLine()) != null) {
                try {
                    points.add(getPoint(line));
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return points;
    }

    /**
     *********************************************************
     ** @desc ：解析每行数据，转换成point对象
     ** @author Pings
     ** @date   2017/11/11
     ** @param  line 每行数据
     ** @return 转换后的点
     ** *******************************************************
     */
    public static Point getPoint(String line) {
        if (line == null || line.trim().length() == 0)
            throw new IllegalArgumentException(line + " is null");

        String[] datas = line.split(";");
        double[] point = Arrays.stream(datas).mapToDouble(data -> Double.parseDouble(data)).toArray();

        Point p = new Point(Arrays.copyOf(point, point.length - 1), point[point.length - 1]);

        return p;
    }

    public static void main(String[] args) {
        long start = System.currentTimeMillis();

        List<Point> trainDatas = parseData("D:\\java\\source\\Pings\\machinelearning\\machinelearning\\src\\main\\java\\knn\\data-training.txt");
        List<Point> testDatas = parseData("D:\\java\\source\\Pings\\machinelearning\\machinelearning\\src\\main\\java\\knn\\data-test.txt");

        testDatas.forEach(testData -> KNN.getRst(trainDatas, testData, 5, 1));

        long end = System.currentTimeMillis();
        System.out.println("计算时长：" + (end - start) + "毫秒") ;
    }
}
