package ch.ethz;

import sun.misc.Unsafe;

import java.io.UnsupportedEncodingException;

import static ch.ethz.Main.readString;
import static sun.misc.Unsafe.getUnsafe;

/**
 */
public class Customer {
    private final int cId;
    private final int dId;
    private final int wId;
    private final String cFirst;
    private final String cLast;

    Customer (int c, int d, int w, String first, String last) {
        cId = c;
        dId = d;
        wId = w;
        cFirst = first;
        cLast = last;
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

    public int getdId() {
        return dId;
    }

    public int getwId() {
        return wId;
    }

    public String getcFirst() {
        return cFirst;
    }

    public String getcLast() {
        return cLast;
    }
}
