// ColumnStoreAbstract.h

#ifndef COLUMNSTOREABSTRACT_H
#define COLUMNSTOREABSTRACT_H

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <ctime>

using namespace std;

// An abstract class representing a column store.
class ColumnStoreAbstract {
    public:
        static const int STRING_DATATYPE = 0;
        static const int INTEGER_DATATYPE = 1;
        static const int FLOAT_DATATYPE = 2;
        static const int TIME_DATATYPE = 3;
        static const string DTFORMATSTRING;

        // The registered column headers with this column store.
        unordered_set<string> columnHeaders;

        // The registered column headers, together with its data type.
        // Only the following data types are accepted:
        //
        // STRING_DATATYPE,
        // INTEGER_DATATYPE,
        // FLOAT_DATATYPE and
        // TIME_DATATYPE.
        unordered_map<string, int> columnDataTypes;

        // User has to specify, for each column, 1. the column name 2. the corresponding data type.
        ColumnStoreAbstract(unordered_map<string, int> columnDataTypes);

        // Parses the CSV file and stores into the column store, using storeAll()
        void addCSVData(string filepath);

        // Given a value string and the corresponding column, store into data storage.
        virtual void store(string column, string value) = 0;

        // Given a map of columns to its values (in String), store all into the data storage.
        virtual void storeAll(unordered_map<string, vector<string>> buffer) = 0;

        // Scans all the indexes of the column and returns the indexes whose values match the predicate.
        virtual vector<int> filter(string column, bool predicate) = 0;

        // Scans the given indexes of the column and returns the indexes whose values match the predicate.
        virtual vector<int> filter(string column, bool predicate, vector<int> indexesToCheck) = 0;

        // Scans the given indexes of the column and returns the indexes whose values are the largest among all the scanned values.
        //
        // The implementation by extending classes is recommended to perform a validation check to ensure column type is a number.
        virtual vector<int> getMax(string column, vector<int> indexesToCheck) = 0;

        // Scans the given indexes of the column and returns the indexes whose values are the smallest among all the scanned values.
        //
        // The implementation by extending classes is recommended to perform a validation check to ensure column type is a number.
        virtual vector<int> getMin(string column, vector<int> indexesToCheck) = 0;

        // Returns the name of this column store.
        virtual string getName() = 0;

        // Gets the value from a column based on the index.
        virtual Object* getValue(string column, int index) = 0;

        // Prints the head of the data (i.e. from index 0) until the specified index.
        virtual void printHead(int until) = 0;

        // Useful in getMax() and getMin() functions.
        // So that code does not have to be repeated.
        bool validationCheckForMinMax(string column);
    
    protected:
         // Based on the value string and column type, cast this value string to the appropriate type.
         // Additionally, checks the validation of value string.
         Object castValueAccordingToColumnType(string column, string value);

         // Checks if the column was registered with this column store or not.
         bool isInvalidColumn(string column);
};

// A class representing an object that can hold different types of values
class Object {
    public:
        enum Type {NONE, STRING, INT, FLOAT, TIME, BOOL};

        Type type;
        string sval;
        int ival;
        float fval;
        tm tval;
        bool flag;

        Object();

        Object(string s);

        Object(int i);

        Object(float f);

        Object(tm t);

        Object(bool flag);
};

#endif