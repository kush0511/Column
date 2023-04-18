// basic file operations
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

/**
 * A general column store implementation where the data is stored in disk.
 */
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
        ColumnStoreDisk(unordered_map<string, int> columnDataTypes) : ColumnStoreAbstract(columnDataTypes){
            this->columnDataTypes = columnDataTypes;
            // Get the column headers from the map keys
            for (auto& pair : columnDataTypes) {
                columnHeaders.insert(pair.first);
            }
        }

        

        // Write an appropriate value to the outputStream given the column string and value string
        void store(ofstream& outputStream, string column, string value) {
            Object* toAdd = &castValueAccordingToColumnType(column, value);
            switch(columnDataTypes[column]) {
                case STRING_DATATYPE: {
                    // Get its string value, each datum separated by new line
                    if (toAdd == nullptr) {
                        outputStream << "M\n";
                    } else {
                        outputStream << *reinterpret_cast<string*>(toAdd) << "\n";
                    }
                    break;
                }
                case TIME_DATATYPE: {
                    if (toAdd == nullptr) {
                        outputStream << "M\n";
                    } else {
                        outputStream << chrono::system_clock::to_time_t(*reinterpret_cast<chrono::system_clock::time_point*>(toAdd)) << "\n";
                    }
                    break;
                }
                case INTEGER_DATATYPE: {
                    if (toAdd == nullptr) { handleStoreInteger(outputStream, INT_MIN); }
                    else { handleStoreInteger(outputStream, *reinterpret_cast<int*>(toAdd)); }
                    break;
                }
                case FLOAT_DATATYPE: {
                    if (toAdd == nullptr) { handleStoreFloat(outputStream, NAN); }
                    else { handleStoreFloat(outputStream, *reinterpret_cast<float*>(toAdd)); }
                    break;
                }
                default: outputStream << "M\n";
            }
        }

        // Write a value to a file given the column and value strings
        void store(string column, string value) {
            try {
                ofstream outputStream(getName() + "/" + column + ".store", ios::app | ios::binary);
                store(outputStream, column, value);
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
        }

        // Write multiple values to multiple files given a buffer of columns and values
        void storeAll(map<string, vector<string>> buffer) {
            try {
                for (auto& pair : buffer) {
                    string column = pair.first;
                    ofstream outputStream(getName() + "/" + column + ".store", ios::app | ios::binary);
                    for (string& value : pair.second) {
                        store(outputStream, column, value);
                    }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
        }

        // Filter a column by a predicate and return a list of row indexes that satisfy it
        vector<int> filter(string column, function<bool(Object*)> predicate) {
            vector<int> result;
            try {
                ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                int idx = 0;
                if (isNotNumberDataType(column)) { // Use getline since it's string
                    string value;
                    while (getline(inputStream, value)) {
                        Object* toCheck;
                        if (value != "M") {
                            if (columnDataTypes[column] == TIME_DATATYPE) {
                                toCheck = reinterpret_cast<Object*>(new chrono::system_clock::time_point(chrono::system_clock::from_time_t(stoi(value))));
                            } else {
                                toCheck = reinterpret_cast<Object*>(new string(value));
                            }
                            if (predicate(toCheck)) { result.push_back(idx); }
                        }
                        idx++;
                    }
                } else { // Use read
                    char buffer[4];
                    while (inputStream.read(buffer, 4)) {
                        Object* toCheck = convertBytesToNumber(buffer, columnDataTypes[column]);
                        if (toCheck != nullptr && predicate(toCheck)) {
                            result.push_back(idx);
                        }
                        idx++;
                    }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return result;
        }

        // Filter a column by a predicate and a list of row indexes to check and return a list of row indexes that satisfy it
        vector<int> filter(string column, function<bool(Object*)> predicate, vector<int> indexesToCheck) {
            vector<int> result;
            try {
                if (isNotNumberDataType(column)) {
                    ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                    int currIndex = 0;
                    for (int indexToCheck : indexesToCheck) {
                        Object* toCheck;
                        while (currIndex != indexToCheck) {
                            if (!inputStream.ignore(numeric_limits<streamsize>::max(), '\n')) {
                                // End of file reached
                                cerr << "Index to check is out of bounds!" << endl;
                                return result;
                            }
                            currIndex++;
                        }
                        string value;
                        getline(inputStream, value);
                        currIndex++;
                        if (value == "M") { continue; } // Null value, predicate will always be false. Can skip to next index to check
                        if (columnDataTypes[column] == STRING_DATATYPE) { toCheck = reinterpret_cast<Object*>(new string(value)); }
                        else { toCheck = reinterpret_cast<Object*>(new chrono::system_clock::time_point(chrono::system_clock::from_time_t(stoi(value)))); }

                        if (predicate(toCheck)) { result.push_back(indexToCheck); }
                    }
                } else { // Values are stored directly, each taking up 4 bytes. Can skip to index using seekg
                    ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                    char buffer[4];
                    for (int indexToCheck : indexesToCheck) {
                        // Can access directly
                        inputStream.seekg(indexToCheck * 4L);
                        if (!inputStream.read(buffer, 4)) {
                            cerr << "Did not read 4 bytes when getting a number value from file." << endl;
                        }
                        Object* toCheck = convertBytesToNumber(buffer, columnDataTypes[column]);
                        if (toCheck != nullptr && predicate(toCheck)) { result.push_back(indexToCheck); }
                    }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return result;
        }

        // Get the maximum value(s) of a column from a list of row indexes to check and return a list of row indexes that have the maximum value
        vector<int> getMax(string column, vector<int> indexesToCheck) {
            vector<int> result;
            if (!validationCheckForMinMax(column)) { return result; }

            try {
                ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                char buffer[4];
                float maximum = FLT_MIN;
                float valueAtIndex;
                for (int indexToCheck : indexesToCheck) {
                    inputStream.seekg(indexToCheck * 4L);
                    if (!inputStream.read(buffer, 4)) {
                        cerr << "Did not read 4 bytes when getting a number value from file." << endl;
                    }
                    Object* objectAtIndex = convertBytesToNumber(buffer, columnDataTypes[column]);
                    if (objectAtIndex == nullptr) { continue; }
                    valueAtIndex = *reinterpret_cast<float*>(objectAtIndex); // Cast to float. Even if the underlying object is int, e.g. 24, it will cast to 24.0. It does NOT read the underlying bytes as float, still reads as int
                    if (valueAtIndex == maximum) {
                        result.push_back(indexToCheck);
                    } else if (valueAtIndex > maximum) {
                        result.clear();
                        result.push_back(indexToCheck);
                        maximum = valueAtIndex; // Update new maximum
                    }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return result;
        }

        // Get the minimum value(s) of a column from a list of row indexes to check and return a list of row indexes that have the minimum value
        vector<int> getMin(string column, vector<int> indexesToCheck) {
            vector<int> result;
            if (!validationCheckForMinMax(column)) {
                return result;
            }

            try {
                ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                char buffer[4];
                float minimum = FLT_MAX;
                float valueAtIndex;
                for (int indexToCheck : indexesToCheck) {
                    inputStream.seekg(indexToCheck * 4L);
                    if (!inputStream.read(buffer, 4)) {
                        cerr << "Did not read 4 bytes when getting a number value from file." << endl;
                    }
                    Object* objectAtIndex = convertBytesToNumber(buffer, columnDataTypes[column]);
                    if (objectAtIndex == nullptr) { continue; }
                    valueAtIndex = *reinterpret_cast<float*>(objectAtIndex); // Cast to float. Even if the underlying object is int, e.g. 24, it will cast to 24.0. It does NOT read the underlying bytes as float, still reads as int
                    if (valueAtIndex == minimum) {
                        result.push_back(indexToCheck);
                    } else if (valueAtIndex < minimum) {
                        result.clear();
                        result.push_back(indexToCheck);
                        minimum = valueAtIndex; // Update new minimum
                    }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return result;
        }

        // Get the name of the column store
        string getName() {
            return "disk";
        }

        // Get the value of a column at a given row index
        Object* getValue(string column, int index) {
            if (isInvalidColumn(column)) {
                cerr << "Invalid column" << endl;
                return nullptr;
            }

            try {
                if (isNotNumberDataType(column)) {
                    // Values are stored as string, separated by newlines
                    // Cannot skip index, must manually call getline
                    ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                    string value;
                    while (index > 0 && getline(inputStream, value)) {
                        index--;
                    }
                    getline(inputStream, value);
                    if (value == "M") { return nullptr; }
                    if (columnDataTypes[column] == STRING_DATATYPE) { return reinterpret_cast<Object*>(new string(value)); }
                    else {
                        return reinterpret_cast<Object*>(new chrono::system_clock::time_point(chrono::system_clock::from_time_t(stoi(value))));
                    }
                } else { // Values are stored directly, each taking up 4 bytes. Can skip to index using seekg
                    ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                    char buffer[4];
                    inputStream.seekg(index * 4L);
                    if (!inputStream.read(buffer, 4)) {
                        cerr << "Did not read 4 bytes when getting a number value from file." << endl;
                    }
                    return convertBytesToNumber(buffer, columnDataTypes[column]);
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return nullptr;
        }

        // Print the first n values of each column
        void printHead(int n) {
            try {
                for (const string& column : columnHeaders) {
                    cout << column << ": ";
                    if (isNotNumberDataType(column)) {
                        ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                        string value;
                        int temp = n;
                        while (temp > 0 && getline(inputStream, value)) {
                            cout << value << ",";
                            temp--;
                        }
                    } else {
                        ifstream inputStream(getName() + "/" + column + ".store", ios::binary);
                        char buffer[4];
                        int temp = n;
                        while (temp > 0 && inputStream.read(buffer, 4)) {
                            cout << convertBytesToNumber(buffer, columnDataTypes[column]) << ",";
                            temp--;
                        }
                    }
                    cout << endl;
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
        }

               // Converts the byte array (should be of size 4) into either a float or int based on the data type passed in
        // If the converted number is INT_MIN or NAN, then return nullptr
        Object* convertBytesToNumber(char* buffer, int dataType) {
            Object* result;
            if (dataType == INTEGER_DATATYPE) {
                int value;
                memcpy(&value, buffer, 4); // Copy the buffer to the int value
                if (value == INT_MIN) { return nullptr; } // The design is such that INT_MIN is equivalent to null
                result = reinterpret_cast<Object*>(new int(value));
            } else if (dataType == FLOAT_DATATYPE) {
                float value;
                memcpy(&value, buffer, 4); // Copy the buffer to the float value
                if (isnan(value)) { return nullptr; } // The design is such that NAN is equivalent to null
                result = reinterpret_cast<Object*>(new float(value));
            } else {
                cerr << "Wrong usage of this function (convertBytesToNumber). Should pass in only FLOAT or INTEGER dataType." << endl;
                return nullptr;
            }
            return result;
        }

        // Append 8 bytes to the file representing a long
        void handleStoreLong(ofstream& fileOutputStream, long value) {
            char buffer[8];
            memcpy(buffer, &value, 8); // Copy the long value to the buffer
            fileOutputStream.write(buffer, 8); // Write the buffer to the file
        }

        // Append 4 bytes to the file representing an int
        void handleStoreInteger(ofstream& fileOutputStream, int value) {
            char buffer[4];
            memcpy(buffer, &value, 4); // Copy the int value to the buffer
            fileOutputStream.write(buffer, 4); // Write the buffer to the file
        }

        // Append 4 bytes to the file representing a float
        void handleStoreFloat(ofstream& fileOutputStream, float value) {
            char buffer[4];
            memcpy(buffer, &value, 4); // Copy the float value to the buffer
            fileOutputStream.write(buffer, 4); // Write the buffer to the file
        }

        // Check if a column is not a number data type
        bool isNotNumberDataType(string column) {
            return columnDataTypes[column] == STRING_DATATYPE || columnDataTypes[column] == TIME_DATATYPE;
        }

        // Check if a column is invalid
        bool isInvalidColumn(string column) {
            return columnDataTypes.find(column) == columnDataTypes.end();
        }

        // Check if a column is valid for min/max operations
        bool validationCheckForMinMax(string column) {
            if (isInvalidColumn(column)) {
                cerr << "Invalid column" << endl;
                return false;
            }
            if (isNotNumberDataType(column)) {
                cerr << "Cannot perform min/max on non-number data type" << endl;
                return false;
            }
            return true;
        }

        // private:
        //     // Map of column names and data types
        //     map<string, int> columnDataTypes;

        //     // Vector of column names
        //     vector<string> columnHeaders;

        //     // Cast a value string to an appropriate object according to the column data type
        //     Object* too(string column, string value) {
        //         switch(columnDataTypes[column]) {
        //             case STRING_DATATYPE: return reinterpret_cast<Object*>(new string(value));
        //             case TIME_DATATYPE: return reinterpret_cast<Object*>(new chrono::system_clock::time_point(chrono::system_clock::from_time_t(stoi(value))));
        //             case INTEGER_DATATYPE: return reinterpret_cast<Object*>(new int(stoi(value)));
        //             case FLOAT_DATATYPE: return reinterpret_cast<Object*>(new float(stof(value)));
        //             default: return nullptr;
        //         }
        //     }
};