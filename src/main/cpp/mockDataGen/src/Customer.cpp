#include <cstdint>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include "DataSource.h"
#include "Customer.h"
//#include <string>
#include <ch_ethz_NativeTester.h>

using namespace std;

Customer** Customer::allocateCustomers(int n) {
	int dId = 10;
	int wId = 100;
	Customer** cc = (Customer **) malloc (sizeof(Customer *)*n);
	for (int i = 0; i < n; i++) {
		cc[i] = (Customer*)malloc(sizeof(Customer));
		createCustomer(i, dId, wId);
	}
}

Customer *Customer::createCustomer(int cId, int dId, int wId) {
	void* mem = malloc (sizeof(Customer)); // allocating raw memory
	Customer* c = new (mem) Customer();	// doing a "placement new"
	c->cId = cId; 	//C_ID
	c->dId = dId;	//C_D_ID
	c->wId = wId;	//C_W_ID

	c->cFirst = DataSource::genAlphanumeric64(8,16);		//C_FIRST
	c->cState = DataSource::randomAlphanumeric62(2);		//C_STATE
	return c;
}

long Customer::serializeCustomers(Customer *c, int n) {
	printCustomer(c);
	int nf = 3; // numeric fields
	int sf = 2; // alphanumeric fields
	char* cont = new char[nf*sizeof(int32_t)*sf + c->cFirst.size() + c->cState.size()];
	// copy numeric fields
	int offset = 0;
	memcpy(cont, &c->cId, sizeof(c->cId));
	offset += sizeof(int32_t);
	memcpy(cont+offset, &c->dId, sizeof(c->dId));
	offset += sizeof(int32_t);
	memcpy(cont+offset, &c->wId, sizeof(c->wId));
	// copy alphanumeric fields
//	uint32_t sz1 = c->cFirst.size();

//	memcpy(cont + nf*sizeof(c->cId), &sz1, sizeof(sz1));
//	memcpy(cont + (nf+1)*sizeof(int32_t), c->cFirst.data(), sz1);

//	int offset = (nf+1)*sizeof(int32_t) + sz1;
//	uint32_t sz2 = c->cState.size();
//	memcpy(cont + offset + sizeof(c->cId), &sz2, sizeof(sz2));
//	offset += sizeof(c->cId);
//	memcpy(cont + offset, c->cState.data(), sz2);

//	std::string str("hell world!");
//    int32_t i = 42;
//    char* res = new char[2*sizeof(int32_t) + str.size()];
//    memcpy(res, &i, sizeof(i));
//
//    uint32_t sz = str.size();
//    memcpy(res + sizeof(i), &sz, sizeof(sz));
//    memcpy(res + 2*sizeof(int32_t), str.data(), sz);
//    return reinterpret_cast<long>(res);
    return reinterpret_cast<long>(cont);
}

void Customer::printCustomer(Customer *c) {
	printf("Customer:%d - %d - %d - %s - %s\n", c->cId, c->dId, c->wId, c->cFirst.c_str(), c->cState.c_str());
}