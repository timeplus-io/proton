# How to connect to Proton via JDBC

This docker compose file demonstrates how to connect to Proton via JDBC driver.

## Start Proton with data generator

Simply run `docker compose up` in this folder. Two docker containers in the stack:

1. d.timeplus.com/timeplus-io/proton:latest, as the streaming database. Port 8123 is exposed so that JDBC driver can connect to it.
2. timeplus/cardemo:latest, as the data generator

Please note port 8123 from the Proton container is exposed to the host. You need this port to connect to Proton via JDBC driver.

## Connect to Proton via JDBC driver

Proton JDBC driver is available on maven central repository. `com.timeplus:proton-jdbc:0.6.0`

You can also download the latest JDBC driver from https://github.com/timeplus-io/proton-java-driver/releases

Assuming you have experience with JDBC, the key information you need are:
* JDBC URL is `jdbc:proton://localhost:8123` or `jdbc:proton://localhost:8123/default`
* Username is `default` and password is an empty string
* Driver class is `com.timeplus.proton.jdbc.ProtonDriver`

Please note, by default Proton's query behavior is streaming SQL, looking for new data in the future and never ends. This can be considered as hang for JDBC client. You have 2 options:
1. Use the 8123 port. In this mode, all SQL are ran in batch mode. So `select .. from car_live_data` will read all existing data.
2. Use 3218 port. In this mode, by default all SQL are ran in streaming mode. Please use `select .. from .. LIMIT 100` to stop the query at 100 events. Or use the `table` function to query historical data, such as `select .. from table(car_live_data)..`

### Example Java code

```java
package test_jdbc_driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class App {
    static int query(String url, String user, String password, String table) throws SQLException {
        String sql = "select * from " + table+" limit 10";
        System.out.println(sql);
        try (Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
                //ResultSet rs = stmt.executeQuery("select 1")) {
            int count = 0;
            while (rs.next()) {
                count++;
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
            }
            return count;
        }
    }
    public static void main(String[] args) {
        String url = "jdbc:proton://localhost:8123";
        String user = System.getProperty("user", "default");
        String password = System.getProperty("password", "");
        String table = "car_live_data";

        try {
            query(url, user, password, table);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
```

### Connnect to Proton via DBeaver

You can also connect to Proton from GUI tools that supports JDBC, such as DBeaver.

First add the Proton JDBC driver to DBeaver. Taking DBeaver 23.2.3 as an example, choose "Driver Manager" from "Database" menu. Click the "New" button, and use the following settings:
* Driver Name: Timeplus Proton
* Driver Type: Generic
* Class Name: com.timeplus.proton.jdbc.ProtonDriver
* URL Tempalte: jdbc:proton://{host}:{port}/{database}
* Default Port: 8123
* Default Database: default
* Default User: default
* Allow Empty Password

In the "Libaries" tab, click "Add Artifact" and type `com.timeplus:proton-jdbc:0.6.0`. Click the "Find Class" button to load the class.

Create a new database connection, choose "Timeplus Proton" and accept the default settings. Click the "Test Connection.." to verify the connection is okay.

Open a SQL script for this connection, type the sample SQL `select count() from table(car_live_data)` Ctrl+Enter to run the query and get the result.
