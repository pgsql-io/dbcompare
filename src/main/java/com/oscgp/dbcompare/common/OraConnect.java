/*
 * OSCG-Partners
 * 2022 All rights reserved 
 */
package com.oscgp.dbcompare.common;

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

    public OraConnect() {
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
        String getTablesQuery = "SELECT table_name FROM dba_tables WHERE owner='" + schemaName + "' ORDER BY table_name";
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(getTablesQuery);
            while (rs.next()) {
                tableNames.add(rs.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tableNames;
    }

    public int getTableRowCount(String schemaName, String tableName) {
        int rowCount = 0;
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) \n"
                    + "from " + schemaName + "." + tableName);
            while (rs.next()) {
                rowCount = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rowCount;
    }

    public int getTableIndexCount(String schemaName, String tableName) {
        int count = 0;
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("select count(*) \n"
                    + "from dba_indexes where TABLE_OWNER = '" + schemaName + "' and TABLE_NAME = '" + tableName + "'");
            while (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }

    public void writeTablesRowCount(String[] schemaNames) {
        String oraFileName = "ora.csv";
        File oraFile = new File(oraFileName);
        oraFile.delete();

        try {
            FileWriter fWriter = new FileWriter(oraFile, false);
            for (String schemaName : schemaNames) {
                schemaName = schemaName.toUpperCase();
                ArrayList<String> tableNames = getSchemaTables(schemaName);
                for (String tableName : tableNames) {
                    int rowCount = getTableRowCount(schemaName, tableName);
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
