#include "DataSource.h"
#include "Customer.h"

Customer** Customer::allocateCustomers(int n);
	Customer** cc = (Customer **) malloc (sizeof(Customer *)*n);
	for (int i = 0; i < n; i++) {
		cc[i] = (Customer*)malloc(sizeof(Customer));
		createCustomer(cc[i]);
	}
}

Customer *Customer::createCustomer(int& cId, int& dId, int& wId, Customer* c){
	if(cId<=1000)											//C_ID
		c->cId = DataSource::genCLast(cId-1,cLast);
	else
		c->cId = DataSource::randomCLast(cLast);

	c->dId;													//C_D_ID
	c->wId;													//C_W_ID
	c->cFirst = DataSource::genAlphanumeric64(8,16);		//C_FIRST
	c->cState = DataSource::randomAlphanumeric62(2);		//C_STATE

	return c;
}