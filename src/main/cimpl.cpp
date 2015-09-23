#include <ch_ethz_NativeTester.h>
#include <string>
#include <cstdint>
#include <memory.h>

jlong Java_ch_ethz_NativeTester_createStruct(JNIEnv*, jobject) {
    // this small test just fills a memory region with an int and a string
    std::string str("Hello World!");
    int32_t i = 42;
    char* res = new char[2*sizeof(int32_t) + str.size()];
    memcpy(res, &i, sizeof(i));
    uint32_t sz = str.size();
    memcpy(res + sizeof(i), &sz, sizeof(sz));
    memcpy(res + 2*sizeof(int32_t), str.data(), sz);
    return reinterpret_cast<long>(res);
}

void Java_ch_ethz_NativeTester_deleteStruct(JNIEnv*, jobject, jlong addr) {
    char* s = reinterpret_cast<char*>(addr);
    delete[] s;
}