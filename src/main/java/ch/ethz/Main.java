package ch.ethz;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.io.UnsupportedEncodingException;

public class Main {

    private static Unsafe getUnsafe() throws IllegalAccessException, NoSuchFieldException {
        Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
        singleoneInstanceField.setAccessible(true);
        return (Unsafe) singleoneInstanceField.get(null);
    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException, UnsupportedEncodingException {
        Unsafe u = getUnsafe();
        NativeTester tester = new NativeTester();
        long s = tester.createStruct();
        System.out.println("Address found in " + s);
        // read a int32 from the C++ heap
        int offset = 0;
        int cId = u.getInt(s);
        System.out.printf("cId-> %d\n", cId);
        // read a string from the C++ heap
        offset += 4;
        int dId = u.getInt(s + offset);
        System.out.printf("dId-> %d\n", dId);
        offset += 4;
        int wId = u.getInt(s + offset);
        System.out.printf("wId-> %d\n", wId);
        offset += 4;
        int sz = u.getInt(s + offset);
        System.out.printf("string1 size-> %d\n", sz);
        offset += 4;
        byte[] str = new byte[sz];
        for (int i = 0; i < sz; ++i) {
            str[i] = u.getByte(s + offset + i);
        }
        System.out.printf("String:--%s--\n", new String(str, "UTF-8"));
        offset += sz;
        int sz2 = u.getInt(s + offset);
        System.out.printf("string2 size-> %d\n", sz2);
        offset += 4;
        System.out.printf("String:--%s--\n", readString(u, s +offset, sz2));
        tester.deleteStruct(s);
    }
    public static String readString(Unsafe u, long add, int length) throws UnsupportedEncodingException {
        byte[] str = new byte[length];
        for (int i = 0; i < length; ++i) {
            str[i] = u.getByte(add + i);
        }
        return new String(str, "UTF-8");
    }
}