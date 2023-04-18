// ColumnStoreMM.h

#ifndef COLUMNSTOREMM_H
#define COLUMNSTOREMM_H

#include <iostream>
#include <vector>
#include <map>
#include <functional>
#include "ColumnStoreAbstract.h"


using namespace std;

// a column store implementation where the data is stored in main memory
class ColumnStoreMM : public ColumnStoreAbstract {
    private:
        map<string, vector<Object>> data; // a map of column names to vectors of objects

    public:
        // constructor that takes a map of column names and data types
        ColumnStoreMM(unordered_map<string, int> columnDataTypes);

        // store a value in a column
        void store(string column, string value) override;

        // store all values in a buffer
        void storeAll(unordered_map<string, vector<string>> buffer) override;

        // filter a column by a predicate and return the indexes of matching values
        vector<int> filter(string column, bool predicate) override;

        // filter a column by a predicate and return the indexes of matching values from a given list of indexes
        vector<int> filter(string column, bool predicate, vector<int> indexesToCheck);

        // get the maximum value in a column from a given list of indexes
        vector<int> getMax(string column, vector<int> indexesToCheck) override;

        // get the minimum value in a column from a given list of indexes
        vector<int> getMin(string column, vector<int> indexesToCheck) override;

        // get the name of the storage type
        string getName() override;

        // get values of a specfic cell
        Object* getValue(string column, int index);

        // print the first few values of each column
        void printHead(int until) override;
};

#endif
