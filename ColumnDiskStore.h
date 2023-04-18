#ifndef COLUMN_DISK_STORE_H
#define COLUMN_DISK_STORE_H
// header file content goes here
#endif

#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <functional>
#include <vector>
#include <cfloat>
#include <map>
#include "ColumnStoreAbstract.h"

using namespace std;

class ColumnStoreDisk: public ColumnStoreAbstract {
    public:
        // Constants for data types
        static const int STRING_DATATYPE = 0;
        static const int TIME_DATATYPE = 1;
        static const int INTEGER_DATATYPE = 2;
        static const int FLOAT_DATATYPE = 3;

        // Buffer size when reading files
        static const int BUFFER_SIZE = 10240;

        // Date time format string
        static const string DTFORMATSTRING;

        // Constructor
        using ColumnStoreAbstract::ColumnStoreAbstract;
        ColumnStoreDisk(map<string, int> columnDataTypes);

        // Write an appropriate value to the outputStream given the column string and value string
        void store(ofstream& outputStream, string column, string value);

        // Write a value to a file given the column and value strings
        void store(string column, string value);

        // Write multiple values to multiple files given a buffer of columns and values
        void storeAll(map<string, vector<string>> buffer);

        // Filter a column by a predicate and return a list of row indexes that satisfy it
        vector<int> filter(string column, function<bool(Object*)> predicate);

        // Filter a column by a predicate and a list of row indexes to check and return a list of row indexes that satisfy it
        vector<int> filter(string column, function<bool(Object*)> predicate, vector<int> indexesToCheck);

        // Get the maximum value(s) of a column from a list of row indexes to check and return a list of row indexes that have the maximum value
        vector<int> getMax(string column, vector<int> indexesToCheck);

        // Get the minimum value(s) of a column from a list of row indexes to check and return a list of row indexes that have the minimum value
        vector<int> getMin(string column, vector<int> indexesToCheck);

        // Get the name of the column store
        string getName();

        // Get the value of a column at a given row index
        Object* getValue(string column, int index);

        // Print the first n values of each column
        void printHead(int n);
};

