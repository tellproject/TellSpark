#include <cstdint>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include "DataSource.h"
#include "Customer.h"

using namespace std;

Customer** Customer::allocateCustomers(int n) {
	int dId = 10;
	int wId = 100;
	Customer** cc = (Customer **) malloc (sizeof(Customer *)*n);
	for (int i = 0; i < n; i++) {
		cc[i] = (Customer*)malloc(sizeof(Customer));
		createCustomer(i, dId, wId, cc[i]);
	}
}

Customer *Customer::createCustomer(int cId, int dId, int wId, Customer* c) {
	void* mem = malloc (sizeof(Customer)); // allocating raw memory
	c = new (mem) Customer();	// doing a "placement new"
	c->cId = cId; 		//C_ID
	c->dId = dId;													//C_D_ID
	c->wId = wId;													//C_W_ID
	c->cFirst = DataSource::genAlphanumeric64(8,16);		//C_FIRST
	c->cState = DataSource::randomAlphanumeric62(2);		//C_STATE

	return c;
}

long Customer::serializeCustomers(Customer **c, int n) {
	std::string str("Hello World!");
    int32_t i = 42;
    char* res = new char[2*sizeof(int32_t) + str.size()];
    memcpy(res, &i, sizeof(i));
    uint32_t sz = str.size();
    memcpy(res + sizeof(i), &sz, sizeof(sz));
    memcpy(res + 2*sizeof(int32_t), str.data(), sz);
    return reinterpret_cast<long>(res);
}

void Customer::printCustomer(Customer *c) {
	printf("Customer:%d - %d - %d - %s - %s", c->cId, c->dId, c->wId, c->cFirst.c_str(), c->cState.c_str());

}