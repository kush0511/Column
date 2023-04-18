// ColumnStoreDiskEnhanced.h

#ifndef COLUMNSTOREDISKENHANCED_H
#define COLUMNSTOREDISKENHANCED_H

#include <string>
#include <list>
#include <map>
#include <fstream>
#include "ColumnDiskStore.h"
#include "Output.h"

class ColumnStoreDiskEnhanced : public ColumnStoreDisk {
public:
    // Constructor
    ColumnStoreDiskEnhanced(std::unordered_map<std::string, int> columnDataTypes);

    // Override store method
    void store(std::ofstream& outputStream, std::string column, std::string value);

    // Override getName method
    std::string getName() override;

    // Get the extreme values of Max temp, min temp, max humidity, min humidity for each month, in the year and station specified
    std::list<Output> getExtremeValues(int year, std::string station);

private:
    // Constants for null and station values
    static const byte NULL_STATION;
    static const byte PAYA_LEBAR_STATION;
    static const byte CHANGI_STATION;
    static const long NULL_TIMESTAMP;

    // Constants for min and max keys
    static const std::string MIN_KEY;
    static const std::string MAX_KEY;

    // Custom implementation to store values from column "Station"
    void handleStoreStation(std::ofstream& fileOutputStream, std::string stationName);

    // Custom implementation to store values from column "Timestamp"
    void handleStoreTimestamp(std::ofstream& fileOutputStream, long timeStamp);

    // Scans the "Timestamp" column and returns the indexes whose time matches the year input
    std::list<int> getYear(int year);

    // Scans the indexes in the given list for the column "Station", and returns the indexes whose value matches the station input
    std::list<int> getStation(std::string station, std::list<int> indexesToCheck);

    // Scans the indexes in the given list for the column "Timestamp", and returns the indexes whose time matches the month input
    std::list<int> getMonth(int month, std::list<int> indexesToCheck);

    // Scans the indexes in the given list for the column given, and returns the maximum and minimum values among all the indexes scanned
    std::map<std::string, std::list<int>> sharedScanningMaxMin(std::string column, std::list<int> indexesToCheck);

    // Scans the indexes in the given list, gets those indexes that matches the month given,
    // and finds the extreme values (min/max humidity/temperature) within these indexes
    void scanValues(int month, std::list<int> qualifiedIndexes, std::list<Output>& results, std::string station);

    // For each index in the given list: scan its value using the given fileInput,
    // get its timestamp using the given timeFile,
    // checks if the date of this timestamp has already been added into the results list. If yes, skip this index.
    // Else, create a new Output object based on value, timestamp, station given and output type given and add it to results
    void addResults(std::list<Output>& results, std::list<int> indexes,
                    std::ifstream& fileInput, std::ifstream& timeFile,
                    std::string station, int type);

    // Concatenate a list with the list to modify in a synchronized manner to prevent concurrency issues
    void addToListSync(std::list<Output>& list, std::list<Output>& toAdd);
};

#endif