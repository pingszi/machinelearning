package common.arithmetic;

/**
 *********************************************************
 ** @desc  ： 距离算法实现
 ** @author  Pings
 ** @date    2017/11/11
 ** @version v1.0
 * *******************************************************
 */
public class DistanceUtil {

    //**R值，默认为1（街区距离）
    public static long R = 1;

    /**
     *********************************************************
     ** @desc ：获取两点之间的街区距离
     ** @author Pings
     ** @date   2017/11/11
     ** @param d1 点1
     ** @param d2 点2
     ** @return 两点之间的距离
     ** *******************************************************
     */
    public static double getDistance(double[] d1, double[] d2) {
        return getDistance(d1, d2, R);
    }

    /**
     *********************************************************
     ** @desc ：获取两点之间的距离
     ** @author Pings
     ** @date   2017/11/11
     ** @param d1 点1
     ** @param d2 点2
     ** @param r  R值
     ** @return 两点之间的距离
     ** *******************************************************
     */
    public static double getDistance(double[] d1, double[] d2, Long r) {
        //**验证数据
        if(d1 == null || d1.length == 0 && d2 == null && d2.length == 0)
            throw new IllegalArgumentException("d1或d2为空");

        if(d1.length != d2.length)
            throw new IllegalArgumentException("d1和d2长度不相等");

        if(r == null || r <= 0) r = DistanceUtil.R;

        return calculate(d1, d2, r);
    }

    /**
     *********************************************************
     ** @desc ：计算两点之间的距离
     ** @author Pings
     ** @date   2017/11/11
     ** @param d1 点1
     ** @param d2 点2
     ** @param r  R值
     ** @return 两点之间的距离
     ** *******************************************************
     */
    private static double calculate(double[] d1, double[] d2, long r) {
        double rst = 0.0d;

        for(int i = 0; i < d1.length; i++) {
            rst += Math.pow(Math.abs(d1[i] - d2[i]), r);
        }

        return Math.pow(rst, 1d/r);
    }

}
