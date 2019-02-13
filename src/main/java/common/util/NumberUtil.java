package common.util;

/**
 *********************************************************
 ** @desc  ： 数学函数
 ** @author  Pings
 ** @date    2017/12/5
 ** @version v1.0
 * *******************************************************
 */
public class NumberUtil {

    /**
     *********************************************************
     ** @desc ：求对数
     ** @author Pings
     ** @date   2017/12/05
     ** @param  value 原始数
     ** @param  base 底数
     ** @return 以base为底value的对数
     ** *******************************************************
     */
     public static double log(double value, double base) {
        return Math.log(value) / Math.log(base);
     }

    /**
     *********************************************************
     ** @desc ：求对数
     ** @author Pings
     ** @date   2017/12/05
     ** @param  value 原始数
     ** @return 以2为底value的对数
     ** *******************************************************
     */
    public static double log2(double value) {
        return log(value, 2);
    }
}
