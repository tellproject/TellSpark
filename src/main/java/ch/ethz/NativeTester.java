package ch.ethz;

public class NativeTester {
    static {
        System.loadLibrary("cimpl");
    }

    public native long createStruct();
    public native void deleteStruct(long address);
}
