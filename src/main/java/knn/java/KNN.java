package knn.java;

import common.arithmetic.DistanceUtil;

import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *********************************************************
 ** @desc  ： KNN算法实现
 ** @author  Pings
 ** @date    2017/11/11
 ** @version v1.0
 * *******************************************************
 */
public class KNN {

    private static Logger logger = LogManager.getLogger(KNN.class);

    //**K值,默认为1（取最近的1个样本数据）
    public static int K = 1;

    /**
     *********************************************************
     ** @desc ：计算指定点的结果(K=1,R=1)
     ** @author Pings
     ** @date   2017/11/11
     ** @param  points 点集合
     ** @param  point  指定点
     ** @return 指定点的结果
     ** *******************************************************
     */
    public static Object getRst(List<Point> points, Point point) {
        return getRst(points, point, K, 1);
    }

    /**
     *********************************************************
     ** @desc ：计算指定点的结果(R=1)
     ** @author Pings
     ** @date   2017/11/11
     ** @param  points 点集合
     ** @param  point  指定点
     ** @param  k      K值(取最近的个样本数据)
     ** @return 指定点的结果
     ** *******************************************************
     */
    public static Object getRst(List<Point> points, Point point, int k) {
        return getRst(points, point, k, 1);
    }

    /**
     *********************************************************
     ** @desc ：计算指定点的结果(K=1)
     ** @author Pings
     ** @date   2017/11/11
     ** @param  points 点集合
     ** @param  point  指定点
     ** @param  r      R值(使用哪种距离)
     ** @return 指定点的结果
     ** *******************************************************
     */
    public static Object getRst(List<Point> points, Point point, long r) {
        return getRst(points, point, K, r);
    }

    /**
     *********************************************************
     ** @desc ：计算指定点的结果
     ** @author Pings
     ** @date   2017/11/11
     ** @param  points 点集合
     ** @param  point  指定点
     ** @param  k      K值(取最近的个样本数据)
     ** @param  r      R值(使用哪种距离)
     ** @return 指定点的结果
     ** *******************************************************
     */
    public static Object getRst(List<Point> points, Point point, int k, long r) {
        if (points.size() <= k) k = K;

        Point.Distance rst;

        //**计算指定点和点集合中每个点之间的距离
        Point p = calculate(points, point, r);

        //**获取最近的K个距离
        Point.Distance[] ds = p.getD();
        Point.Distance[] kDs = Arrays.copyOf(ds, k);

        if(k == 1)
            rst = kDs[0];
        else
            rst = mode(kDs); //**计算距离的众数

        logger.info("计算结果：" + rst);

        return rst.getRst();
    }

    /**
     *********************************************************
     ** @desc ：计算指定点和点集合中每个点之间的距离
     ** @author Pings
     ** @date   2017/11/11
     ** @param  points 点集合
     ** @param  point  指定点
     ** @param  r      R值(使用哪种距离)
     ** @return 指定点
     ** *******************************************************
     */
    private static Point calculate(List<Point> points, Point point, long r) {
        logger.info("计算距离..." );

        Point.Distance[] ds = new Point.Distance[points.size()];

        for(int i = 0; i < points.size(); i++) {
            double distance = DistanceUtil.getDistance(points.get(i).getP(), point.getP(), r);
            ds[i] = new Point.Distance(distance, points.get(i).getRst());

            logger.info("离第"+ i +"个点之间的距离：" + distance);
        }

        Arrays.sort(ds);
        point.setD(ds);

        logger.info("距离从小到大排序："+ Arrays.toString(ds));

        return point;
    }

    /**
     *********************************************************
     ** @desc ：计算距离的众数
     ** @author Pings
     ** @date   2017/11/11
     ** @param  ds 距离集合
     ** @return 众数
     ** *******************************************************
     */
    private static Point.Distance mode(Point.Distance[] ds) {
        logger.info("计算众数..." );

        Map<Point.Distance, Integer> countMap = new HashMap<>();

        //**计算每个距离出现的次数
        for (Point.Distance d : ds) {
            int count = countMap.containsKey(d) ? countMap.get(d) + 1 : 1;
            countMap.put(d, count);

            logger.info("距离："+ d + "：" + d.getRst() +"出现的次数，" + count);
        }

        //**计算出现次数最多的距离
        Map.Entry<Point.Distance, Integer>[] counts = countMap.entrySet().toArray(new Map.Entry[countMap.entrySet().size()]);
        Arrays.sort(counts, new Comparator<Map.Entry<Point.Distance, Integer>>() {
            @Override
            public int compare(Map.Entry<Point.Distance, Integer> o1, Map.Entry<Point.Distance, Integer> o2) {
                return o1.getValue() - o2.getValue() < 0 ? -1 : o1.getValue() - o2.getValue() == 0 ? 0 : -1;
            }
        });

        logger.info("众数从小到大排序："+ Arrays.toString(counts));

        return counts[counts.length - 1].getKey();
    }
}