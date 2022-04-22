/*
 * OSCG-Partners
 * 2022 All rights reserved 
 */
package com.oscgp.dbcompare.common;

import static com.oscgp.dbcompare.common.Utility.printLog;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author asif
 */
public class OraConnect {

    DatabaseMetaData metadata = null;
    Connection con = null;
    boolean verbose = false;

    public OraConnect() {
    }

    public OraConnect(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean connectOracle(String connectionString, String user, String password) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager
                    .getConnection("jdbc:oracle:thin:@" + connectionString,
                            user, password);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            return false;
        }
        System.out.println("Opened Oracle database successfully");
        try {
            metadata = con.getMetaData();
            System.out.println("Database version : " + metadata.getDatabaseProductVersion());
        } catch (SQLException ex) {
            Logger.getLogger(OraConnect.class.getName()).log(Level.WARNING, null, ex);
        }
        return true;
    }

    public Connection getConnection() {
        return con;
    }

    public String getDatabaseProductName() {
        try {
            return metadata.getDatabaseProductName();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getDriverName() {
        try {
            return metadata.getDriverName();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getUserName() {
        try {
            return metadata.getUserName();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getDatabaseProductVersion() {
        try {
            return metadata.getUserName();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getDriverVersion() {
        try {
            return metadata.getDriverVersion();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String getURL() {
        try {
            return metadata.getURL();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int getDriverMinorVersion() {
        return metadata.getDriverMinorVersion();
    }

    public int getDriverMajorVersion() {
        return metadata.getDriverMajorVersion();
    }

    public int getDatabaseMinorVersion() {
        try {
            return metadata.getDatabaseMinorVersion();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public int getDatabaseMajorVersion() {
        try {
            return metadata.getDatabaseMajorVersion();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public int getJDBCMajorVersion() {
        try {
            return metadata.getJDBCMajorVersion();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public int getJDBCMinorVersion() {
        try {
            return metadata.getJDBCMinorVersion();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void getSchemas() {
        System.out.println("get schemas info ......");
        try {
            ResultSet rs = metadata.getSchemas();
            ResultSetMetaData rsMeta = rs.getMetaData();
            while (rs.next()) {
                System.out.println("\n----------");
                System.out.println(rs.getString("TABLE_SCHEM"));
                System.out.println("----------");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void getTables() {
        System.out.println("get tables info ......");

        try {
            ResultSet rs = metadata.getTables(null, "SYSTEM", "%", null);

            ResultSetMetaData rsMeta = rs.getMetaData();
            int columnCount = rsMeta.getColumnCount();

            while (rs.next()) {

                System.out.println("\n----------");
                System.out.println(rs.getString("TABLE_NAME"));
                System.out.println("----------");

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = rsMeta.getColumnName(i);
                    System.out.format("%s:%s\n", columnName, rs.getString(i));
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("get tables info ...... done ");
    }

    public ArrayList<String> getSchemaTables(String schemaName) {
        ArrayList<String> tableNames = new ArrayList<String>();
        String query = "SELECT table_name FROM dba_tables WHERE owner='" + schemaName + "' AND nested = 'NO' AND iot_type IS NULL AND table_name NOT LIKE '%$%' ORDER BY table_name";
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            printLog(query, verbose);
            while (rs.next()) {
                tableNames.add(rs.getString(1));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tableNames;
    }

    public long getTableRowCount(String schemaName, String tableName) {
        long rowCount = 0;
        String query = "SELECT count(*) "
                    + "FROM " + schemaName + "." + tableName;
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            printLog(query, verbose);
            while (rs.next()) {
                rowCount = rs.getLong(1);
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rowCount;
    }

    public int getTableIndexCount(String schemaName, String tableName) {
        int count = 0;
        String query = "SELECT count(*) "
                    + "FROM dba_indexes WHERE table_owner = '" + schemaName + "' AND table_name = '" + tableName + "'";
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            printLog(query, verbose);
            while (rs.next()) {
                count = rs.getInt(1);
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    public void writeTablesInfo(String[] schemaNames) {
        String oraFileName = "ora.csv";
        File oraFile = new File(oraFileName);
        oraFile.delete();

        try {
            FileWriter fWriter = new FileWriter(oraFile, false);
            for (String schemaName : schemaNames) {
                schemaName = schemaName.toUpperCase();
                ArrayList<String> tableNames = getSchemaTables(schemaName);
                for (String tableName : tableNames) {
                    long rowCount = getTableRowCount(schemaName, tableName);
                    int indexCount = getTableIndexCount(schemaName, tableName);
                    fWriter.write(schemaName.toLowerCase() + "."
                            + tableName.toLowerCase() + ","
                            + rowCount + "," + indexCount + "\n");
                }
            }
            System.out.println("Oracle stats file (CSV) '" + oraFileName + "' created");
            fWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
