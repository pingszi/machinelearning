package knn.java;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 *********************************************************
 ** @desc  ： 点数据
 ** @author  Pings
 ** @date    2017/11/11
 ** @version v1.0
 * *******************************************************
 */
public class Point {

    //**点数据
    private double[] p;
    //**点数据的结果
    private Object rst;
    //**点数据和其它每个点的距离
    private Distance[] d;

    public Point(double[] p, Object rst) {
        this.p = p;
        this.rst = rst;
    }

    public double[] getP() {
        return p;
    }

    public void setP(double[] p) {
        this.p = p;
    }

    public Object getRst() {
        return rst;
    }

    public void setRst(Object rst) {
        this.rst = rst;
    }

    public Distance[] getD() {
        return d;
    }

    public void setD(Distance[] d) {
        this.d = d;
    }

    @Override
    public String toString() {
        return "data:" + Arrays.toString(p) + ", rst:" + rst;
    }

    /**
     *********************************************************
     ** @desc  ： 两点之间的距离
     ** @author  Pings
     ** @date    2017/11/11
     ** @version v1.0
     * *******************************************************
     */
    public static class Distance implements Comparable<Distance>, Writable {
        private double d;
        private Object rst;

        public Distance() {}

        public Distance(double d, Object rst) {
            this.d = d;
            this.rst = rst;
        }

        public double getD() {
            return d;
        }

        public void setD(double d) {
            this.d = d;
        }

        public Object getRst() {
            return rst;
        }

        public void setRst(Object rst) {
            this.rst = rst;
        }

        public void set(double d, Object rst) {
            this.d = d;
            this.rst = rst;
        }

        @Override
        public int compareTo(Distance o) {
            return this.d - o.d < 0 ? -1 : this.d - o.d  == 0 ? 0 : 1;
        }

        @Override
        public String toString() {
            return "{" +"d=" + d + ", rst=" + rst + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Distance distance = (Distance) o;

            return rst != null ? rst.equals(distance.rst) : distance.rst == null;
        }

        @Override
        public int hashCode() {
            return rst != null ? rst.hashCode() : 0;
        }


        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(this.d);
            dataOutput.writeUTF(this.rst.toString());
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.d = dataInput.readDouble();
            this.rst = dataInput.readUTF();
        }
    }
}
