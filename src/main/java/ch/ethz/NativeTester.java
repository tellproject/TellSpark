package ch.ethz;

public class NativeTester {
    static {
        System.loadLibrary("customer");
    }

    public native long createStruct();
    public native void deleteStruct(long address);
}
