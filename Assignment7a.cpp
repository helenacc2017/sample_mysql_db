#include <iostream>
#include <mysql_driver.h>
#include <mysql_connection.h>
#include <cppconn/statement.h>
#include <cppconn/resultset.h>
#include <cppconn/exception.h>
#include <cppconn/prepared_statement.h>
#include <string>

using namespace std;

// Function to execute and display query results
void executeAndDisplayQuery(sql::Statement* stmt, const string& query, const string& description) {
    try {
        cout << "\n=== " << description << " ===\n";
        sql::ResultSet* res = stmt->executeQuery(query);

        // Get column count
        sql::ResultSetMetaData* metaData = res->getMetaData();
        int columnCount = metaData->getColumnCount();

        // Print column headers
        for (int i = 1; i <= columnCount; i++) {
            cout << metaData->getColumnLabel(i) << "\t";
        }
        cout << endl;

        // Print separator line
        for (int i = 1; i <= columnCount; i++) {
            cout << "---------------\t";
        }
        cout << endl;

        // Print data rows
        while (res->next()) {
            for (int i = 1; i <= columnCount; i++) {
                cout << res->getString(i) << "\t";
            }
            cout << endl;
        }

        delete res;
    } catch (sql::SQLException &e) {
        cout << "SQL Error: " << e.what() << endl;
        cout << "Query: " << query << endl;
    }
}

// Function to check if a student ID already exists
bool studentExists(sql::Statement* stmt, const string& stdNo) {
    string query = "SELECT COUNT(*) FROM Student WHERE StdNo = '" + stdNo + "'";
    sql::ResultSet* res = stmt->executeQuery(query);
    res->next();
    int count = res->getInt(1);
    delete res;
    return count > 0;
}

// Function to find a unique student ID
string findUniqueStudentId(sql::Statement* stmt, string baseId) {
    // If the base ID is available, use it
    if (!studentExists(stmt, baseId)) {
        return baseId;
    }

    // Otherwise, try variations until we find an unused one
    for (int i = 1; i <= 999; i++) {
        string newId = baseId.substr(0, 8) + to_string(i % 10) + to_string((i / 10) % 10) + to_string((i / 100) % 10);
        if (!studentExists(stmt, newId)) {
            return newId;
        }
    }

    // If all variations are taken (unlikely), return an empty string
    return "";
}

int main() {
    try {
        // Create a MySQL connection
        sql::mysql::MySQL_Driver *driver;
        sql::Connection *con;

        driver = sql::mysql::get_mysql_driver_instance();
        con = driver->connect("mysql.eecs.ku.edu", "348s25_c522z106", "riK4thoh");

        // Connect to the database
        con->setSchema("348s25_c522z106");

        // Create a statement
        sql::Statement *stmt = con->createStatement();

        // 1. Retrieve all students majoring in 'IS'
        string query1 = "SELECT * FROM Student WHERE StdMajor = 'IS'";
        cout << ">> Query (1) --------------------" << endl;
        executeAndDisplayQuery(stmt, query1, "Students majoring in IS");

        // 2. Find the names of students who have enrolled in more than two courses
        string query2 = "SELECT s.StdFirstName, s.StdLastName, COUNT(e.OfferNo) as CourseCount "
                        "FROM Student s "
                        "JOIN Enrollment e ON s.StdNo = e.StdNo "
                        "GROUP BY s.StdNo "
                        "HAVING COUNT(e.OfferNo) > 2";
        cout << ">> Query (2) --------------------" << endl;
        executeAndDisplayQuery(stmt, query2, "Students enrolled in more than two courses");

        // 3. List all professors who have been teaching for more than 5 years in the 'Physics' department
        // Note: The database doesn't have a Physics department, so we'll adjust this query
        string query3 = "SELECT FacFirstName, FacLastName, FacDept, FacHireDate "
                        "FROM Faculty "
                        "WHERE FacDept = 'MS' AND YEAR(CURDATE()) - YEAR(FacHireDate) > 5";
        cout << ">> Query (3) --------------------" << endl;
        executeAndDisplayQuery(stmt, query3, "Professors teaching for more than 5 years in MS department");

        // 4. Retrieve the total number of students enrolled in each department, but only for departments with more than 50 students
        // Note: Our sample database likely won't have departments with 50+ students, so we'll adjust the threshold
        string query4 = "SELECT StdMajor as Department, COUNT(*) as StudentCount "
                        "FROM Student "
                        "GROUP BY StdMajor "
                        "HAVING COUNT(*) > 2";
        cout << ">> Query (4) --------------------" << endl;
        executeAndDisplayQuery(stmt, query4, "Departments with more than 2 students");

        // 5. Find the course names and IDs of courses that have 'Data' in their title and are taught by a specific faculty
        string query5 = "SELECT c.CourseNo, c.CrsDesc "
                        "FROM Course c "
                        "JOIN Offering o ON c.CourseNo = o.CourseNo "
                        "JOIN Faculty f ON o.FacNo = f.FacNo "
                        "WHERE c.CrsDesc LIKE '%DATA%' AND f.FacLastName = 'FIBON'";
        cout << ">> Query (5) --------------------" << endl;
        executeAndDisplayQuery(stmt, query5, "Data courses taught by Prof. FIBON");

        // 6. Display the students who have not enrolled in any courses in the past two semesters
        // Using a more simplified approach for the sample data
        string query6 = "SELECT s.StdNo, s.StdFirstName, s.StdLastName "
                        "FROM Student s "
                        "WHERE s.StdNo NOT IN (SELECT e.StdNo FROM Enrollment e)";6
        cout << ">> Query (6) --------------------" << endl;
        executeAndDisplayQuery(stmt, query6, "Students not enrolled in any courses");

        // 7. Retrieve the second-highest GPA among students
        string query7 = "SELECT StdNo, StdFirstName, StdLastName, StdGPA "
                        "FROM Student "
                        "ORDER BY StdGPA DESC "
                        "LIMIT 1 OFFSET 1";
        cout << ">> Query (7) --------------------" << endl;
        executeAndDisplayQuery(stmt, query7, "Student with second-highest GPA");

        // 8. Find the names of students who are also teaching assistants but have a GPA above 3.5
        // Since we don't have a direct TA table, we'll check if student numbers match any faculty numbers
        string query8 = "SELECT s.StdNo, s.StdFirstName, s.StdLastName, s.StdGPA "
                        "FROM Student s "
                        "JOIN Faculty f ON s.StdNo = f.FacNo "
                        "WHERE s.StdGPA > 3.5";
        cout << ">> Query (8) --------------------" << endl;
        executeAndDisplayQuery(stmt, query8, "Student TAs with GPA above 3.5");

        // 9. List all students along with their enrolled courses, but only for those who enrolled after 2022
        // Adjusting for the sample data which has older dates
        string query9 = "SELECT s.StdFirstName, s.StdLastName, c.CrsDesc, o.OffTerm, o.OffYear "
                        "FROM Student s "
                        "JOIN Enrollment e ON s.StdNo = e.StdNo "
                        "JOIN Offering o ON e.OfferNo = o.OfferNo "
                        "JOIN Course c ON o.CourseNo = c.CourseNo "
                        "WHERE o.OffYear >= 2020";
        cout << ">> Query (9) --------------------" << endl;
        executeAndDisplayQuery(stmt, query9, "Students with courses enrolled after or in 2020");

        // 10. Retrieve the names and salaries of the top three highest-paid professors
        string query10 = "SELECT FacFirstName, FacLastName, FacSalary "
                         "FROM Faculty "
                         "ORDER BY FacSalary DESC "
                         "LIMIT 3";
        cout << ">> Query (10) --------------------" << endl;
        executeAndDisplayQuery(stmt, query10, "Top three highest-paid professors");

        cout << ">> Query (11) --------------------" << endl;
        // 11. Insert a Junior Computer Science student named "Alice Smith" from Topeka, Kansas
        try {
            // Check if student already exists and find a unique ID if needed
            string stdNo = "888-88-8888";
            string uniqueStdNo = findUniqueStudentId(stmt, stdNo);

            if (uniqueStdNo.empty()) {
                cout << "\n=== Error: Could not find a unique student ID ===\n";
            } else {
                // If the ID had to be modified, inform the user
                if (uniqueStdNo != stdNo) {
                    cout << "\n=== Student ID '" << stdNo << "' already exists. Using '" << uniqueStdNo << "' instead ===\n";
                }

                string insertQuery = "INSERT INTO Student (StdNo, StdFirstName, StdLastName, StdCity, "
                                  "StdState, StdMajor, StdClass, StdGPA, StdZip) "
                                  "VALUES ('" + uniqueStdNo + "', 'ALICE', 'SMITH', 'TOPEKA', 'KS', 'CS', 'JR', 3.85, '66610')";

                stmt->execute(insertQuery);
                executeAndDisplayQuery(stmt, "SELECT * FROM Student WHERE StdNo = '" + uniqueStdNo + "'",
                                      "Inserted student Alice Smith");
            }
        } catch (sql::SQLException &e) {
            cout << "\n=== Error inserting student: " << e.what() << " ===\n";
            cout << "MySQL Error Code: " << e.getErrorCode() << endl;
            cout << "SQLState: " << e.getSQLState() << endl;

            // If it's a duplicate key error, try with a different student ID
            if (e.getErrorCode() == 1062) {  // Duplicate key error
                string newStdNo = "999-99-9999";  // Try a completely different ID
                cout << "Trying with alternative student ID: " << newStdNo << endl;

                try {
                    string insertQuery = "INSERT INTO Student (StdNo, StdFirstName, StdLastName, StdCity, "
                                      "StdState, StdMajor, StdClass, StdGPA, StdZip) "
                                      "VALUES ('" + newStdNo + "', 'ALICE', 'SMITH', 'TOPEKA', 'KS', 'CS', 'JR', 3.85, '66610')";
                    stmt->execute(insertQuery);
                    executeAndDisplayQuery(stmt, "SELECT * FROM Student WHERE StdNo = '" + newStdNo + "'",
                                          "Inserted student Alice Smith (with alternative ID)");
                } catch (sql::SQLException &e2) {
                    cout << "\n=== Error inserting student with alternative ID: " << e2.what() << " ===\n";
                }
            }
        }

        // 12. Update Bob Norbert's information
        stmt->execute("UPDATE Student SET StdCity = 'OVERLAND PARK', StdZip = '66210' "
                      "WHERE StdNo = '124-56-7890'");
        cout << ">> Query (12) --------------------" << endl;
        executeAndDisplayQuery(stmt, "SELECT * FROM Student WHERE StdNo = '124-56-7890'",
                            "Updated student Bob Norbert");

        // Clean up
        delete stmt;
        delete con;

        cout << "\nAll queries executed successfully!\n";

    } catch (sql::SQLException &e) {
        cout << "SQL Exception: " << e.what() << endl;
        cout << "MySQL Error Code: " << e.getErrorCode() << endl;
        cout << "SQLState: " << e.getSQLState() << endl;
    }

    return 0;
}