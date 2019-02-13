package common.arithmetic;

import common.util.NumberUtil;

import java.util.Arrays;

/**
 *********************************************************
 ** @desc  ： 决策树分类算法
 ** @author  Pings
 ** @date    2017/12/5
 ** @version v1.0
 * *******************************************************
 */
public class DTree {

    /**
     *********************************************************
     ** @desc ：计算熵
     ** @author Pings
     ** @date   2017/12/04
     ** @param  ClaQtys 各种分类的数量
     ** @return 熵
     ** *******************************************************
     */
    public static double entropy(int... ClaQtys) {
        if(ClaQtys == null)
            throw new IllegalArgumentException("ClaQtys不能为空");

        double total = Arrays.stream(ClaQtys).sum();
        return Arrays.stream(ClaQtys).mapToDouble(it -> -it / total * NumberUtil.log2(it / total)).sum();
    }

    /**
     *********************************************************
     ** @desc ：计算Gini指数
     ** @author Pings
     ** @date   2017/12/05
     ** @param  ClaQtys 各种分类的数量
     ** @return Gini指数
     ** *******************************************************
     */
    public static double gini(int... ClaQtys) {
        if(ClaQtys == null)
            throw new IllegalArgumentException("ClaQtys不能为空");

        double total = Arrays.stream(ClaQtys).sum();
        return 1 + Arrays.stream(ClaQtys).mapToDouble(it -> -Math.pow(it/total, 2)).sum();
    }

    public static void main(String[] args) {
        System.out.println(entropy(4, 4) * 8/9);
        //System.out.println(entropy(1, 2) * 3/9);
        //System.out.println(gini(3, 2) * 5/9);
        //System.out.println(gini(2, 2) * 4/9);
    }
}
