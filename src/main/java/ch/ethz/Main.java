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
//        int v = u.getInt(s);
//        System.out.printf("Read %d\n", v);
//        // read a string from the C++ heap
//        int sz = u.getInt(s + 4);
//        byte[] str = new byte[sz];
//        for (int i = 0; i < sz; ++i) {
//            str[i] = u.getByte(s + 8 + i);
//        }
//        System.out.printf("String: %s\n", new String(str, "UTF-8"));
//        tester.deleteStruct(s);
    }
}
