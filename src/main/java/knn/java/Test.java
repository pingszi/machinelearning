package knn.java;

import java.util.Arrays;

/**
 *********************************************************
 ** @desc  ： 测试KNN,计算红酒品质
 ** @author  Pings
 ** @date    2017/11/11
 ** @version v1.0
 * *******************************************************
 */
public class Test {

    public static void main(String[] args) {
        double[] d0 = {7.4,0.70,0.00,1.9,0.076,11.0,34.0,0.9978,3.51,0.56,9.4};
        double[] d1 = {7.8,0.88,0.00,2.6,0.098,25.0,67.0,0.9968,3.20,0.68,9.8};
        double[] d2 = {7.8,0.76,0.04,2.3,0.092,15.0,54.0,0.9970,3.26,0.65,9.8};
        double[] d3 = {11.2,0.28,0.56,1.9,0.075,17.0,60.0,0.9980,3.16,0.58,9.8};
        double[] d4 = {7.4,0.70,0.00,1.9,0.076,11.0,34.0,0.9978,3.51,0.56,9.4};

        Point p0 = new Point(d0, 5);
        Point p1 = new Point(d1, 5);
        Point p2 = new Point(d2, 5);
        Point p3 = new Point(d3, 6);
        Point p4 = new Point(d4, 5);

        Object rst = KNN.getRst(Arrays.asList(p0, p1, p2, p3), p4, 3, 3);
        System.out.println(p4 + "的品质为：" + rst);
    }
}
