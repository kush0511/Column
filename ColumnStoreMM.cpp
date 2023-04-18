// A column store implementation where the data is stored in main memory.
#include <iostream>
#include <vector>
#include <map>
#include <functional>
using namespace std;
#include "ColumnStoreAbstract.h"

class ColumnStoreMM : public ColumnStoreAbstract {
    private:
        map<string, vector<Object>> data; // a map of column names to vectors of objects

    public:
        // constructor that takes a map of column names and data types
        ColumnStoreMM(unordered_map<string, int> columnDataTypes) : ColumnStoreAbstract(columnDataTypes) {
            for (auto& pair : columnHeaders) {
                string columnHeader = pair;
                data[columnHeader] = vector<Object>(); // initialize the data vector with empty objects
            }
        }

        // store a value in a column
        void store(string column, string value) override {
            if (isInvalidColumn(column)) {
                cout << "Column is not registered with this column store." << endl;
            } else {
                Object toAdd = castValueAccordingToColumnType(column, value);
                data[column].push_back(toAdd);
            }
        }

        // store all values in a buffer
        void storeAll(unordered_map<string, vector<string>> buffer) override {
            for (auto& pair : buffer) {
                string column = pair.first;
                vector<string> values = pair.second;
                for (string value : values) {
                    store(column, value);
                }
            }
        }

        // filter a column by a predicate and return the indexes of matching values
        vector<int> filter(string column, bool predicate) override {
            vector<int> results;
            if (isInvalidColumn(column)) {
                cout << "Column is not registered with this column store." << endl;
                return results;
            }

            for (int i = 0; i < data[column].size(); i++) {
                Object value = data[column][i];
                if (predicate) {
                    results.push_back(i);
                }
            }
            return results;
        }

        // filter a column by a predicate and return the indexes of matching values from a given list of indexes
        vector<int> filter(string column, bool predicate, vector<int> indexesToCheck) {
            vector<int> results;
            if (isInvalidColumn(column)) {
                cout << "Column is not registered with this column store." << endl;
                return results;
            }

            for (int index : indexesToCheck) {
                Object value = data[column][index];
                if (predicate) {
                    results.push_back(index);
                }
            }
            return results;
        }

        // get the maximum value in a column from a given list of indexes
        vector<int> getMax(string column, vector<int> indexesToCheck) override {
            vector<int> results;
            if (!validationCheckForMinMax(column)) { return results; } //return empty vector if validation check fails

            float maximum = numeric_limits<float>::min();
            for (int index : indexesToCheck) {
                if (data[column][index].type == NULL) { continue; }
                float value = (float) data[column][index].fval;
                if (value == maximum) {
                    results.push_back(index);
                } else if (value > maximum) {
                    maximum = value;
                    results.clear();
                    results.push_back(index);
                }
            }

            return results;
        }

        // get the minimum value in a column from a given list of indexes
        vector<int> getMin(string column, vector<int> indexesToCheck) override {
            vector<int> results;
            if (!validationCheckForMinMax(column)) { return results; } //return empty vector if validation check fails

            float minimum = numeric_limits<float>::max();
            for (int index : indexesToCheck) {
                if (data[column][index].type == NULL) { continue; }
                float value = (float) data[column][index].fval;
                if (value == minimum) {
                    results.push_back(index);
                } else if (value < minimum) {
                    minimum = value;
                    results.clear();
                    results.push_back(index);
                }
            }

            return results;
        }

        // get the name of the storage type
        string getName() override {
            return "main_memory";
        }

        // print the first few values of each column
        void printHead(int until) override {
            for (string column : columnHeaders) {
                cout << column << ": ";
                for (int i = 0; i < until; i++) {
                    cout << data[column][i].fval << " ";
                }
                cout << endl;
            }
        }
};