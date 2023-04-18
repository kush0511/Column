// This is a C++ code that corresponds to the Java code on this page

#include <iostream>
#include <string>
#include <ctime>

using namespace std;

/**
 * This class represents a row in "ScanResult.csv"
 */
class Output {
    public:
        static const int MAX_HUMIDITY = 0;
        static const int MAX_TEMP = 1;
        static const int MIN_HUMIDITY = 2;
        static const int MIN_TEMP = 3;

        time_t date; // use time_t to store date and time
        string stationName;
        int type;
        float value;

        // default constructor
        Output() {
            date = 0;
            stationName = "";
            type = -1;
            value = 0.0f;
        }

        // parameterized constructor
        Output(time_t date, string station, int type, float value) {
            this->date = date;
            this->stationName = station;
            this->type = type;
            this->value = value;
        }

        // getters
        time_t getDate() {
            return date;
        }

        string getStationName() {
            return stationName;
        }

        int getType() {
            return type;
        }

        float getValue() {
            return value;
        }

        // convert type to string
        string typeToString() {
            string typeString = "";
            switch (type) {
                case MAX_HUMIDITY:
                    typeString = "Max Humidity";
                    break;
                case MAX_TEMP:
                    typeString = "Max Temperature";
                    break;
                case MIN_HUMIDITY:
                    typeString = "Min Humidity";
                    break;
                case MIN_TEMP:
                    typeString = "Min Temperature";
                    break;
                default:
                    typeString = "unknown";
                    break;
            }
            return typeString;
        }

        // override the << operator for printing
        friend ostream& operator<<(ostream& os, const Output& out) {
            // use strftime to format the date
            char buffer[20];
            strftime(buffer, 20, "%Y-%m-%d", localtime(&out.date));
            os << buffer << "," << out.stationName << "," << out.value;
            return os;
        }
};