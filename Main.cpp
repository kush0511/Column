// include necessary headers
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <set>
#include <functional>
#include <chrono>
#include "ColumnStoreAbstract.h" // this is a header file that defines the abstract class
#include "ColumnStoreMM.h" // this is a header file that defines the memory-mapped column store class
#include "ColumnDiskStore.h" // this is a header file that defines the disk-based column store class
#include "ColumnDiskStoreEnhanced.h" // this is a header file that defines the enhanced disk-based column store class
#include "Output.h" // this is a header file that defines the output class

using namespace std;

int main() {
    // create a map of data types
    unordered_map<string, int> dataTypes;
    dataTypes["id"] = ColumnStoreAbstract::INTEGER_DATATYPE;
    dataTypes["Timestamp"] = ColumnStoreAbstract::TIME_DATATYPE;
    dataTypes["Station"] = ColumnStoreAbstract::STRING_DATATYPE;
    dataTypes["Temperature"] = ColumnStoreAbstract::FLOAT_DATATYPE;
    dataTypes["Humidity"] = ColumnStoreAbstract::FLOAT_DATATYPE;

    // create different column store objects
    ColumnStoreAbstract* csMM = new ColumnStoreMM(dataTypes);
    ColumnStoreAbstract* csDisk = new ColumnStoreDisk(dataTypes);
    ColumnStoreAbstract* csDiskEnhanced = new ColumnStoreDiskEnhanced(dataTypes);
    vector<ColumnStoreAbstract*> columnStores {csMM, csDisk, csDiskEnhanced};

    cout << "------Time Taken------" << endl;
    for (ColumnStoreAbstract* cs: columnStores) {
        try {
            cs->addCSVData("SingaporeWeather.csv");
            auto startTime = chrono::system_clock::now();
            vector<Output> results1 = getExtremeValues(cs, 2010, "Changi");
            vector<Output> results2 = getExtremeValues(cs, 2019, "Changi");
            auto endTime = chrono::system_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(endTime - startTime).count();
            cout << cs->getName() << ": " << duration << "ms" << endl;

            writeOutput(cs->getName()+"/ScanResult.csv", results1);
            writeOutput(cs->getName()+"/ScanResult.csv", results2);
        } catch(exception& e) {
            cerr << e.what() << endl;
        }
    }

    // delete the dynamically allocated objects
    delete csMM;
    delete csDisk;
    delete csDiskEnhanced;

    return 0;
}

/**
 * Gets the extreme values for each month in the year specified and station specified.
 * paramater data the column store
 * paramater year the year given
 * paramater station the station given
 * return a vector of Output objects representing the extreme values.
 */
vector<Output> getExtremeValues(ColumnStoreAbstract* data, int year, string station) {
    if (dynamic_cast<ColumnStoreDiskEnhanced*>(data)) {
        return getExtremeValues(data, year, station); //use custom implementation
    }

    vector<int> yearIndices = data->filter("Timestamp", Object(year).flag);
    vector<int> stationAndYearIndices = data->filter("Station", Object(station).flag, yearIndices);
    vector<Output> result;
    for (int month = 1; month <= 12; month++) {
        int m = month; //need to use a final variable in lambda functions
        vector<int> currentMonthIndices = data->filter("Timestamp", Object(station).flag, stationAndYearIndices);
        result.insert(result.end(), processMonth(data, currentMonthIndices, "Humidity", "max", station).begin(), processMonth(data, currentMonthIndices, "Humidity", "min", station).begin());
        result.insert(result.end(), processMonth(data, currentMonthIndices, "Temperature", "max", station).begin(), processMonth(data, currentMonthIndices, "Temperature", "min", station).begin());
    }

    return result;
};

/**
 * gets the extreme values for the specified month in the given column for the given station.
 * paramater data the column store
 * paramater currMonth the vector of indexes representing the current month (and year)
 * paramater column the column given
 * paramater valueType "max" or "min"
 * paramater stationName the station given
 * return a list of Output objects representing the extreme values.
 */
vector<Output> processMonth(ColumnStoreAbstract* data, vector<int> currMonth, string column, string valueType, string stationName) {
    vector<Output> result;
    set<int> addedDays;
    if (column.compare("Humidity") != 0 && column.compare("Temperature") != 0) {
        cout << "This is a specific function that only accepts Humidity or Temperature columns." << endl;
        return result;
    }

    if (valueType.compare("min") != 0 && valueType.compare("max") != 0) {
        cout << "Value type must be 'min' or 'max' only!" << endl;
        return result;
    }

    vector<int> qualifiedIndexes = valueType.compare("max") == 0 ? data->getMax(column, currMonth) : data->getMin(column, currMonth);
    for (int index: qualifiedIndexes) {
        Object* timestamp = data->getValue("Timestamp", index);
        if (addedDays.find(timestamp->fval) != addedDays.end()) {
            continue; // we do not want duplicate days, we only want duplicate months
        } else { addedDays.insert(timestamp->fval); }

        float value = (float) data->getValue(column, index)->fval;
        int outputType = -1;
        if (column.compare("Humidity") == 0) {
            if (valueType.compare("min") == 0) { outputType = Output::MIN_HUMIDITY; }
            else { outputType = Output::MAX_HUMIDITY; }
        } else {
            if (valueType.compare("min") == 0) { outputType = Output::MIN_TEMP; }
            else { outputType = Output::MAX_TEMP; }
        }

        result.push_back(Output(*timestamp, stationName, outputType, value));
    }

    return result;
}

/**
 * for each value in the output vector, write to the file given.
 * paramater filepath file to write
 * paramater toWrite vector of Output objects
 * @throws IOException
 */
void writeOutput(string filepath, vector<Output> toWrite) {
    bool initialized = true;
    ofstream outputFile(filepath, ios::app);
    if (!outputFile.is_open()) {
        initialized = false;
        outputFile.open(filepath);
    }

    if (!outputFile.good()) {
        cout << "Could not set permissions for this file." << endl;
        return;
    }

    if (!initialized) {
        outputFile << "Date,Station,Category,Value\n";
    }

    for (Output data: toWrite) {
        outputFile << data << "\n";
    }

    outputFile.close();
}
