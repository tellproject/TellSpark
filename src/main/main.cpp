#include <ch_ethz_NativeTester.h>
#include "Customer.h"

////// JNI bindings
jlong Java_ch_ethz_NativeTester_createStruct(JNIEnv*, jobject) {
	int cId = 1;
	int dId = 1;
	int wId = 2;
	return Customer::serializeCustomers(Customer::createCustomer(cId, dId,wId), 1);
}

void Java_ch_ethz_NativeTester_deleteStruct(JNIEnv*, jobject, jlong addr) {
    char* s = reinterpret_cast<char*>(addr);
    delete[] s;
}
