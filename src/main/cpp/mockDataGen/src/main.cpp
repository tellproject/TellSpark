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
    Customer *c;
    Customer::createCustomer(cId, dId, wId, c);

        cout<<"Test \n";
        //Customer::printCustomer(c);
        cout << "Hell \n";
        return 0;
}