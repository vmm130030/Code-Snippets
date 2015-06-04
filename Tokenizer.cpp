
#include <cstdlib>
#include <iostream>
#include<fstream>
#include <sstream>
#include <string.h>
using namespace std;

int rows = 0, columns = 0, inputLines = 0;

int retrieveCharNo(char ch, string inputDFA[]) {
    int result = 0;
    for (int j = 0; j < columns - 1; j++) {
        string token = inputDFA[j];
        if (token.size() == 1) {
            if (ch == token[0]) {
                result = j;
            }
        } else if (token.size() == 3) {
            if (token[1] == '-') {
                if (ch >= token[0] && ch <= token[2]) {
                    result = j;
                }
            }
        } else {

            if (token == "space") {
                if (ch == ' ') {
                    result = j;
                }
            } else if (token == "nl") {
                if (ch == '\n') {
                    result = j;
                }
            }
        }
    }
    return result;
}

int tableLookUp(int columnNo, string inputData[]) {
    int result = 0;
    result = atoi(inputData[columnNo].c_str());
    return result;
}

int main(int argc, char** argv) {

    string line;
    ifstream file1("input.dfa");

    while (getline(file1, line)) {
        rows++;
    }
    string lines[rows];

    ifstream file2("input.dfa");
    for (int i = 0; i < rows; i++) {
        getline(file2, line);
        lines[i] = line;
    }

    istringstream str(lines[0]);
    string s;
    while (getline(str, s, '\t')) {
        columns++;
    }

    string inputDFA[rows][columns];

    for (int i = 0; i < rows; i++) {
        istringstream str1(lines[i]);
        string token;
        for (int j = 0; j < columns; j++) {
            getline(str1, token, '\t');
            inputDFA[i][j] = token;
        }
    }

    ifstream file3("input.txt");

    while (getline(file3, line)) {
        inputLines++;
    }

    string inputCode = "";
    ifstream file4("input.txt");
    for (int i = 0; i < inputLines; i++) {
        getline(file4, line);
        inputCode += line;
        inputCode += "\n";
    }

    char inputChar[inputCode.size()];
    for (int i = 0; i < inputCode.size(); i++) {
        inputChar[i] = inputCode[i];
    }

    int start = 0, index = 0, forword = 0;
    int i = 0;
    while (i < inputCode.size() - 1) {
        int col = retrieveCharNo(inputChar[i], inputDFA[0]);
        int state = tableLookUp(col, inputDFA[1]);

        if (state != 0) {
            string tokenType = inputDFA[state][columns - 1];

            bool flag1 = true;
            while (flag1) {
                string currentTokenType = tokenType;
                forword++;
                col = retrieveCharNo(inputChar[forword], inputDFA[0]);
                state = tableLookUp(col, inputDFA[state]);
                tokenType = inputDFA[state][columns - 1];

                if (state == 0) {
                    flag1 = false;
                    if (currentTokenType != "Space-OR-Nl") {
                        for (int j = index; j < forword; j++) {
                            cout << inputChar[j];
                        }
                        cout << "\t" << currentTokenType << endl;
                    }
                    index = forword;
                    i = index;
                }
            }
        } else {
            cout << "Not correct input";
            break;
        }
    }

    return 1;
}

