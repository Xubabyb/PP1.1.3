package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    private static final String URL = "jdbc:mysql://localhost:3306/testUser";
    private static final String LOGIN = "root";
    private static final String PASSWORD = "12345";

    public static Connection connection;

    public static Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = DriverManager.getConnection(URL, LOGIN, PASSWORD);
        }
        return connection;
    }
}
