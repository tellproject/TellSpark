#include <iostream>
#include <string>
#include "Customer.h"
#include "DataSource.h"

using namespace std;

int main ()
{
    int cId = 1;
    int dId = 2;
    int wId = 3;
    Customer *c = Customer::createCustomer(cId, dId, wId);
    cout<<"Test \n";
    Customer::printCustomer(c);
    cout << Customer::serializeCustomers(c, 1) << endl;
    cout << "Hell \n";
    return 0;
}