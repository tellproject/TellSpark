#ifndef CUSTOMER_H
#define CUSTOMER_H

#include <string>

class Customer {
  private:
    int cId;
    int dId;
    int wId;
    std::string cFirst;
    std::string cState;
  public:
    static Customer** allocateCustomers(int n);
    static Customer* createCustomer(int& cId, int& dId, int& wId, Customer* c);
    static void printCustomers(Customer **c, int n);
    static long serializeCustomers(Customer **c, int n);
    static Customer** deserializeCustomers(long l, int n);
};

#endif