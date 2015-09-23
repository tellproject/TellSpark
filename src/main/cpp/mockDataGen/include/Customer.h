#ifndef CUSTOMER_H
#define CUSTOMER_H

#include <string>

class Customer {
  private:
    int32_t cId;
    int32_t dId;
    int32_t wId;
    std::string cFirst;
    std::string cState;
  public:
    static Customer** allocateCustomers(int n);
    static Customer* createCustomer(int cId, int dId, int wId);
    static void printCustomers(Customer **c, int n);
    static void printCustomer(Customer *c);
    static long serializeCustomers(Customer *c, int n);
    static Customer** deserializeCustomers(long l, int n);
};

#endif