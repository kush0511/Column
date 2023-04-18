// This is a header file for the Output class

#ifndef OUTPUT_H
#define OUTPUT_H

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

        // constructors
        Output();
        Output(time_t date, string station, int type, float value);
        Output(Object date, string station, int type, float value);

        // getters
        time_t getDate();
        string getStationName();
        int getType();
        float getValue();

        // convert type to string
        string typeToString();

        // override the << operator for printing
        friend ostream& operator<<(ostream& os, const Output& out);
};

#endif
