package ch.ethz;

import sun.misc.Unsafe;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;

import static ch.ethz.Main.readString;
import static sun.misc.Unsafe.getUnsafe;

/**
 */
public class Customer implements Serializable {

    private static final long serialVersionUID = 7526472295622770144L;

    private int cId;
    private long dId;
    private long wId;
    private String cFirst;
    private String cLast;

    Customer () {}

    Customer (int c, int d, int w, String first, String last) {
        cId = c;
        dId = d;
        wId = w;
        cFirst = first;
        cLast = last;
    }

    public void setField(int fPos, Object val) {
        switch(fPos) {
            case 0: this.cId = (int) val;
                break;
            case 1: this.dId = (long) val;
                break;
            case 2: this.wId = (long) val;
                break;
            case 3: this.cFirst = val.toString();
                break;
            case 4: this.cLast = val.toString();
                break;
        }
    }

    public static Customer deserialize(long memAddrs) throws UnsupportedEncodingException {
        Unsafe u = getUnsafe();
        NativeTester tester = new NativeTester();
        long s = tester.createStruct();
        int offset = 0;
        // read a numbers from c++ memory
        // TODO knowing the schema can improve this?
        int cId = u.getInt(s);
        offset += 4;
        int dId = u.getInt(s + offset);
        offset += 4;
        int wId = u.getInt(s + offset);
        offset += 4;
        // read a string from the C++ heap
        int sz = u.getInt(s + offset);
        offset += 4;
        String first = readString(u, s + offset, sz);
        offset += sz;
        int sz2 = u.getInt(s + offset);
        offset += 4;
        String last = readString(u, s + offset, sz2);
        return new Customer(cId, dId, wId, first, last);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return sb.append("{").append("cId").append(":").append(cId).append(",")
                .append("dId").append(":").append(dId).append(",")
                .append("wId").append(":").append(wId).append(",")
                .append("cFirst").append(":").append(cFirst).append(",")
                .append("cLast").append(":").append(cLast).append("}").toString();
    }

    public int getcId() {
        return cId;
    }

    public long getdId() {
        return dId;
    }

    public long getwId() {
        return wId;
    }

    public String getcFirst() {
        return cFirst;
    }

    public String getcLast() {
        return cLast;
    }
}
