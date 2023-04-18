#include <iostream>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <string>
#include <chrono>
#include <thread>
#include <mutex>
#include "ColumnDiskStore.h"

using namespace std;

/**
 * A specific column store implementation where the data is stored in disk. This is specific to the "SingaporeWeather.csv" input file given.
 *
 * Unlike ColumnStoreDisk and ColumnStoreMM, the class is not for general-use. The class
 * is just to show how to
 * <ul>
 *     <li>Perform shared scanning when calculating extreme values.</li>
 *     <li>Compression of "Station" column from variable width to a fixed 1-byte character.</li>
 *     <li>"Timestamp" values stored as long.</li>
 *     <li>Multi-threaded scans.</li>
 * </ul>
 *
 */
class ColumnStoreDiskEnhanced : public ColumnStoreDisk {
    public:
        /**
         * A byte representing null values for column "Station". This value is 'M' encoded in ASCII.
         */
        static const char NULL_STATION = 'M';

        /**
         * A byte representing "Paya Lebar" for column "Station". This value is 'P' encoded in ASCII.
         */
        static const char PAYA_LEBAR_STATION = 'P';

        /**
         * A byte representing "Changi" for column "Station". This value is 'C' encoded in ASCII.
         */
        static const char CHANGI_STATION = 'C';

        /**
         * 8 bytes representing null values for column "Timestamp". This value is 0L.
         */
        static const long NULL_TIMESTAMP = 0;

        /**
         * For use in {@link #sharedScanningMaxMin(string, vector<int>)}
         */
        static const string MIN_KEY;

        /**
         * For use in {@link #sharedScanningMaxMin(string, vector<int>)}
         */
        static const string MAX_KEY;

        /**
         * {@inheritDoc}
         */
        ColumnStoreDiskEnhanced(unordered_map<string, int> columnDataTypes) : ColumnStoreDisk(columnDataTypes) {}

        /**
         * {@inheritDoc}
         */
        void store(ofstream& outputStream, string column, string value) {
            try {
                Object toStore = castValueAccordingToColumnType(column, value);
                switch(columnDataTypes[column]) {
                    case TIMESTAMP: {
                        if (toStore == null) { handleStoreTimestamp(outputStream, NULL_TIMESTAMP); }
                        else { handleStoreTimestamp(outputStream, (long)toStore);}
                        break;
                    }

                    case STATION: {
                        if (toStore == null) { handleStoreStation(outputStream, NULL_STATION); }
                        else { handleStoreStation(outputStream, (char) toStore);}
                        break;
                    }

                    case ID: {
                        if (toStore == null) { handleStoreInteger(outputStream, INT_MIN); }
                        else { handleStoreInteger(outputStream, (int) toStore); }
                        break;
                    }

                    case TEMPERATURE:
                    case HUMIDITY: {
                        if (toStore == null) { handleStoreFloat(outputStream, NAN); }
                        else { handleStoreFloat(outputStream, (float)toStore); }
                        break;
                    }
                }
            } catch(exception& e) {
                cerr << e.what() << endl;
            }
        }

        /**
         * {@inheritDoc}
         */
        string getName() {
            return "enhanced_disk";
        }

    private:
        /**
         * Custom implementation to store values from column "Station". Stores value as a single byte instead of string.
         * @param fileOutputStream the file to write to
         * @param stationName the value to store
         */
        void handleStoreStation(ofstream& fileOutputStream, char stationName) {
            try {
                fileOutputStream.write((char*)&stationName, sizeof(char));
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
        }

        /**
         * Custom implementation to store values from column "Timestamp". Stores value as a long instead of string.
         * @param fileOutputStream the file to write to
         * @param timeStamp the value to store
         */
        void handleStoreTimestamp(ofstream& fileOutputStream, long timeStamp) {
            try {
                fileOutputStream.write((char*)&timeStamp, sizeof(long));
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
        }

        /**
         * Gets the extreme values of Max temp, min temp, max humidity, min humidity for each month, in the year and station specified.
         *
         * For each month, a thread is run to find these values.
         * @param year the year to check
         * @param station the station to check
         * @return the results
         */
        vector<Output> getExtremeValues(int year, string station) {
            vector<int> qualifiedIndexes = getYear(year);
            qualifiedIndexes = getStation(station, qualifiedIndexes);
            vector<Output> results;

            vector<thread> threads;
            for (int month = 1; month <= 12; month++) {
                int finalMonth = month; // can only pass 'final' variables into lambda function
                vector<int> finalQualifiedIndexes(qualifiedIndexes);
                threads.push_back(thread([this, finalMonth, finalQualifiedIndexes, &results, station]() {
                    scanValues(finalMonth, finalQualifiedIndexes, results, station);
                }));
            }

            for (thread& t: threads) {
                t.join();
            }

            return results;
        }

        /**
         * Scans the "Timestamp" column and returns the indexes whose time matches the year input.
         * @param year the year input
         * @return the matched indexes
         */
        vector<int> getYear(int year) {
            vector<int> results;
            try {
                ifstream inputStream(getName()+"/Timestamp.store", ios::binary);
                char buffer[BUFFER_SIZE];
                int index = 0;
                long startRange = chrono::system_clock::to_time_t(chrono::system_clock::from_time_t(0) + chrono::hours(8) + chrono::years(year - 1970));
                long endRange = chrono::system_clock::to_time_t(chrono::system_clock::from_time_t(0) + chrono::hours(8) + chrono::years(year - 1969) - chrono::seconds(1));
                while (inputStream.read(buffer, BUFFER_SIZE)) {
                    for (int i = 0; i < BUFFER_SIZE; i += 8) {
                        long value = *(long*)(buffer + i);
                        if (value >= startRange && value <= endRange) {
                            results.push_back(index);
                        }
                        index++;
                    }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return results;
        }

        /**
         * Scans the indexes in the given list for the column "Station", and returns the indexes whose value matches the station input.
         * @param station the station input
         * @param indexesToCheck the indexes list given
         * @return the matched indexes
         */
        vector<int> getStation(string station, vector<int> indexesToCheck) {
            vector<int> results;
            try {
                ifstream fileInput(getName()+"/Station.store", ios::binary);
                for(int index: indexesToCheck) {
                    //since station is just 1 byte, can access directly via index
                    fileInput.seekg(index);
                    char value;
                    fileInput.read(&value, sizeof(char));
                    if (station == "Paya Lebar" && value == PAYA_LEBAR_STATION) {
                        results.push_back(index);
                    } else if (station == "Changi" && value == CHANGI_STATION) {
                        results.push_back(index);
                    }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return results;
        }

        /**
         * Scans the indexes in the given list for the column "Timestamp", and returns the indexes whose time matches the month input.
         * @param month the month input
         * @param indexesToCheck the indexes list given
         * @return the matched indexes
         */
        vector<int> getMonth(int month, vector<int> indexesToCheck) {
            vector<int> results;
            try {
                ifstream fileInput(getName()+"/Timestamp.store", ios::binary);
                char buffer[8];
                for(int index: indexesToCheck) {
                    //since timestamp is just 8 bytes, can access directly via index
                    fileInput.seekg(index*8L);
                    fileInput.read(buffer, 8);
                    long value = *(long*)buffer;
                    if (value == NULL_TIMESTAMP) { continue; } //null value
                    time_t time = value;
                    tm* timestamp = localtime(&time);
                    if (timestamp->tm_mon + 1 == month) { results.push_back(index); }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return results;
        }

        /**
         * Scans the indexes in the given list for the column given, and returns the maximum and minimum values among all the indexes scanned.
         * <p>Example of output:
         * { "min": [index1, index2], "max": [index3] }
         * </p>
         * @param column "Humidity" or "Temperature" columns
         * @param indexesToCheck the indexes list given
         * @return the minimum and maximum values
         */
        unordered_map<string, vector<int>> sharedScanningMaxMin(string column, vector<int> indexesToCheck) {
            unordered_map<string, vector<int>> results;
            results[MIN_KEY] = vector<int>();
            results[MAX_KEY] = vector<int>();

            try {
                ifstream fileInput(getName()+"/"+column+".store", ios::binary);
                char buffer[4];
                float min = FLT_MAX;
                float max = FLT_MIN;

                for (int index: indexesToCheck) { //shared scanning
                    fileInput.seekg(index*4L);
                    fileInput.read(buffer, 4);
                    float value = *(float*)buffer;
                    if(isnan(value)) { continue; } // null value

                    if (value == min) { results[MIN_KEY].push_back(index); }
                    else if (value < min) {
                        results[MIN_KEY].clear();
                        results[MIN_KEY].push_back(index);
                        min = value;
                    }

                    if (value == max) { results[MAX_KEY].push_back(index); }
                    else if ( value > max) {
                        results[MAX_KEY].clear();
                        results[MAX_KEY].push_back(index);
                        max =  value;
                    }
                }
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
            return results;
        }

        /**
         * Scans the indexes in the given list, gets those indexes that matches the month given, 
         * and finds the extreme values (min/max humidity/temperature) within
         * these indexes.
         *
         * <p>Creates a new Output object for each extreme value, using the helper function {@link #addResults(vector<Output>&, vector<int>, ifstream&, ifstream&, string, int)}</p>
         * @param month the month given
         * @param qualifiedIndexes the indexes list given
         * @param results to append Output objects to
         * @param station the station given
         */
        void scanValues(int month, vector<int> qualifiedIndexes, vector<Output>& results, string station) {
            vector<int> monthIndexes = getMonth(month, qualifiedIndexes);
            unordered_map<string, vector<int>> scanResultsForTemp = sharedScanningMaxMin("Temperature", monthIndexes);
            unordered_map<string, vector<int>> scanResultsForHumidity = sharedScanningMaxMin("Humidity", monthIndexes);

            try {
                ifstream tempFile(getName()+"/Temperature.store", ios::binary);
                ifstream humidityFile(getName()+"/Humidity.store", ios::binary);
                ifstream timeFile(getName()+"/Timestamp.store", ios::binary);

                addResults(results, scanResultsForHumidity[MAX_KEY], humidityFile, timeFile, station, Output::MAX_HUMIDITY);
                addResults(results, scanResultsForHumidity[MIN_KEY], humidityFile, timeFile, station, Output::MIN_HUMIDITY);
                addResults(results, scanResultsForTemp[MAX_KEY], tempFile, timeFile, station, Output::MAX_TEMP);
                addResults(results, scanResultsForTemp[MIN_KEY], tempFile, timeFile, station, Output::MIN_TEMP);
            } catch (exception& e) {
                cerr << e.what() << endl;
            }
        }

        /**
         * For each index in the given list:
         * <ol>
         *     <li>Scan its value using the given fileInput</li>
         *     <li>Get its timestamp using the given timeFile</li>
         *     <li>Checks if the date of this timestamp has already been added into the results list. If yes, skip this index.</li>
         *     <li>Else, create a new Output object based on value, timestamp, station given and output type given and add it to results.</li>
         * </ol>
         * @param results the list of output objects
         * @param indexes the indexes list given
         * @param fileInput the input file for the indexes
         * @param timeFile the timestamp file
         * @param station the station given
         * @param type the type given
         */
        void addResults(vector<Output>& results,
                        vector<int> indexes,
                        ifstream& fileInput,
                        ifstream& timeFile,
                        string station,
                        int type) {
            try {
                char bufferValue[4];
                char bufferTimestamp[8];

                // because we might get duplicate days, as each day has 48 different times.
                // need to filter out duplicate days
                unordered_set<int> daysAdded;

                for (int index: indexes) {
                    fileInput.seekg(index*4L);
                    timeFile.seekg(index*8L);

                    fileInput.read(bufferValue, 4);
                    timeFile.read(bufferTimestamp, 8);

                    float value = *(float*)bufferValue;
                    long unixTimestamp = *(long*)bufferTimestamp;
                    time_t time = unixTimestamp;
                    tm* timestamp = localtime(&time);
                    if (daysAdded.find(timestamp->tm_mday) == daysAdded.end()) {
                        results.push_back(Output(timestamp, station, type, value));
                        daysAdded.insert(timestamp->tm_mday);
                    }
                }
            } catch(exception& e) {
                cerr << e.what() << endl;
            }
        }

        /**
         * concatenate a list with the list to modify in a synchronized manner to prevent concurrency issues.
         * @param list list to modify
         * @param toAdd the list to add
         */
        void addToListSync(vector<Output>& list, vector<Output>& toAdd) {
            lock_guard<mutex> lock(mtx);
            list.insert(list.end(), toAdd.begin(), toAdd.end());
        }

        mutex mtx;
};

const string ColumnStoreDiskEnhanced::MIN_KEY = "min";
const string ColumnStoreDiskEnhanced::MAX_KEY = "max";
