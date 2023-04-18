#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include <ctime>
#include <sstream>
#include <fstream>


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
        ColumnStoreAbstract(unordered_map<string, int> columnDataTypes) {
            this->columnDataTypes = columnDataTypes;
            this->columnHeaders = unordered_set<string>();
            for (auto& pair : columnDataTypes) {
                columnHeaders.insert(pair.first);
            }
        }

        ColumnStoreAbstract(unordered_map<string, int> columnDataTypes);

        // Parses the CSV file and stores into the column store, using storeAll()
        void addCSVData(string filepath) {
            const string separator = ",";
            ifstream file(filepath);
            if (!file.is_open()) {
                cout << "could not csv decode file: no column headers" << endl;
                return;
            }

            string line;
            if (!getline(file, line)) {
                cout << "could not csv decode file: no column headers" << endl;
                return;
            }

            vector<string> incomingColumnHeaders = split(line, separator); //get the column headers
            if (columnHeaders != unordered_set<string>(incomingColumnHeaders.begin(), incomingColumnHeaders.end())) {
                cout << "Incoming CSV data has different format from current csv data" << endl;
                return;
            }
            unordered_map<string, vector<string>> buffer;
            for (string& column : incomingColumnHeaders) {
                buffer[column] = vector<string>();
            }

            while (getline(file, line)) {
                vector<string> nextDatum = split(line, separator);
                int i = 0;
                while (i < nextDatum.size()) {
                    if (i >= incomingColumnHeaders.size()) { //means this datum has more columns that what is provided, skip to next line
                        break;
                    }

                    string value = nextDatum[i];
                    string column = incomingColumnHeaders[i];
                    buffer[column].push_back(value);
                    i++;
                }

                while (i < incomingColumnHeaders.size()) { //means this datum has less columns that what is provided, add null values
                    buffer[incomingColumnHeaders[i]].push_back("M");
                    i++;
                }
            }

            storeAll(buffer);
            file.close();
        }

        // Given a value string and the corresponding column, store into data storage.
        virtual void store(string column, string value) = 0;

        // Given a map of columns to its values (in String), store all into the data storage.
        virtual void storeAll(unordered_map<string, vector<string>> buffer) = 0;

        // Scans all the indexes of the column and returns the indexes whose values match the predicate.
        virtual vector<int> filter(string column, function<bool(Object)> predicate) = 0;

        // Scans the given indexes of the column and returns the indexes whose values match the predicate.
        virtual vector<int> filter(string column, function<bool(Object)> predicate, vector<int> indexesToCheck) = 0;

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
        virtual Object getValue(string column, int index) = 0;

        // Prints the head of the data (i.e. from index 0) until the specified index.
        virtual void printHead(int until) = 0;

    protected:
        // Checks if the column was registered with this column store or not.
        bool isInvalidColumn(string column) {
            return columnHeaders.find(column) == columnHeaders.end();
        }

        // Based on the value string and column type, cast this value string to the appropriate type.
        // Additionally, checks the validation of value string.
        Object castValueAccordingToColumnType(string column, string value) {
            try {
                if (value == "" || value == "M") {
                    return Object();
                }

                switch (columnDataTypes[column]) {
                    case STRING_DATATYPE: return Object(value);
                    case INTEGER_DATATYPE: return Object(stoi(value));
                    case FLOAT_DATATYPE: return Object(stof(value));
                    case TIME_DATATYPE: return Object(parseTime(value));
                    default: throw invalid_argument("No such data type for column (" + column + ") registered. Defaulting to string...");
                }
            } catch (invalid_argument& e) {
                cout << e.what() << endl;
            } catch (exception& e) { //any other exception
                cout << e.what() << endl;
            }

            return Object();
        }

        // Returns true if column data type is not a integer or float.
        bool isNotNumberDataType(string column) {
            return columnDataTypes[column] != INTEGER_DATATYPE && columnDataTypes[column] != FLOAT_DATATYPE;
        }

        // Useful in getMax() and getMin() functions.
        // So that code does not have to be repeated.
        bool validationCheckForMinMax(string column) {
            if (isInvalidColumn(column)) {
                cout << "Column is not registered with this column store." << endl;
                return false;
            }

            if (isNotNumberDataType(column)) {
                cout << "Cannot perform get max operation on a column whose data are not numbers." << endl;
                return false;
            }

            return true;
        }

    private:
        // Splits a string by a delimiter and returns a vector of tokens
        vector<string> split(string s, string delimiter) {
            vector<string> tokens;
            size_t pos = 0;
            while ((pos = s.find(delimiter)) != string::npos) {
                tokens.push_back(s.substr(0, pos));
                s.erase(0, pos + delimiter.length());
            }
            tokens.push_back(s);
            return tokens;
        }

        // Parses a string into a tm struct representing time
        tm parseTime(string s) {
            tm t;
            stringstream ss(s);
            ss >> get_time(&t, DTFORMATSTRING.c_str());
            return t;
        }
};

// A class representing an object that can hold different types of values
class Object {
    public:
        enum Type {NONE, STRING, INT, FLOAT, TIME};

        Type type;
        string sval;
        int ival;
        float fval;
        tm tval;

        Object() : type(NONE) {}

        Object(string s) : type(STRING), sval(s) {}

        Object(int i) : type(INT), ival(i) {}

        Object(float f) : type(FLOAT), fval(f) {}

        Object(tm t) : type(TIME), tval(t) {}

};