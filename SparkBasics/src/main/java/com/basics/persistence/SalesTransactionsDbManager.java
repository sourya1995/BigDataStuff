package com.basics.persistence;

import org.slf4j.LoggerFactory;
import scala.sys.Prop;

import javax.swing.plaf.nimbus.State;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class SalesTransactionsDbManager {
    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(SalesTransactionsDbManager.class);
    public static final String TABLE_NAME = "INIT";
    public static final String DB_URL = "jdbc:derby:firstdb;create=true;user=app;password=derby";
    public static final String EMBEDDED_DRIVER_STRING = "org.apache.derby.jdbc.EmbeddedDriver";

    public void startDB() throws SQLException {
        Connection conn = DriverManager.getConnection(DB_URL);
        createTable(conn);
    }

    public Properties buildDBProperties(){
        Properties prop = new Properties();
        prop.setProperty("driver", "org.apache.derby.jdbc.EmbeddedDriver");
        prop.setProperty("user", "app");
        prop.setProperty("password", "derby");
        return prop;
    }

    public void displayTableResults(int numRecords) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        int printed = 0;
        Class.forName(EMBEDDED_DRIVER_STRING).newInstance();
        conn = DriverManager.getConnection(DB_URL);
        statement = conn.prepareStatement("SELECT * FROM " + TABLE_NAME);
        resultSet = statement.executeQuery();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnsNumber = resultSetMetaData.getColumnCount();

        while(resultSet.next() && printed < numRecords){
            for (int i = 1; i < columnsNumber ; i++) {
                if(i > 1) System.out.println(", ");
                String columnValue = resultSet.getString(i);
                LOGGER.info("COLUMN: " + resultSetMetaData.getColumnName(i) +  " - VALUE:  " + columnValue);

            }
            LOGGER.info("================================");
            printed++;
        } close(conn, statement, resultSet);

    }

    private void close(Connection conn, PreparedStatement statement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
        }
    }

    private void createTable(Connection connection) throws SQLException {
        try(Statement statement = connection.createStatement()){
            String query = "CREATE TABLE INIT " + "(id integer) ";
            statement.executeUpdate(query);
        } catch(SQLException e){
            e.printStackTrace();
        }
    }
}
