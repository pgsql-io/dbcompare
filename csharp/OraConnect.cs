using System;
using System.Collections.Generic;
using System.IO;
using Oracle.ManagedDataAccess.Client;

//using System.Data.OracleClient;

namespace OSCGP.DBCompare
{
    public class OraConnect
    {
        OracleConnection con;
        Boolean verbose = false;
        List<String> schemaNames = null;

        public OraConnect(Boolean verbose)
        {
            this.verbose = verbose;
        }

        public void setSchemaNames(List<String> schemaNames)
        {
            this.schemaNames = schemaNames;
        }

        public void run()
        {
            connectOra(Config.getProperty("ora.connect"), Config.getProperty("ora.user"), Config.getProperty("ora.password"));
            writeTablesInfo(schemaNames);
            writeViewsInfo(schemaNames);
            writeMViewsInfo(schemaNames);
        }

        public Boolean connectOra(String connectionString, String user, String password)
        {
            try
            {
                con = new OracleConnection(connectionString + "User Id=" + user + ";Password=" + password);
                con.Open();
                Console.WriteLine("Opened Oracle database successfully");
                Console.WriteLine("Oracle Database version : " + con.ServerVersion);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine("Unable to connect to Oracle {0}", e);
            }
            return true;
        }

        public List<String> getSchemaTables(String schemaName)
        {
            List<String> tableNames = new List<String>();
            String query = "SELECT table_name FROM dba_tables WHERE owner='" + schemaName + "' AND nested = 'NO' AND iot_type IS NULL AND table_name NOT LIKE '%$%' ORDER BY table_name";
            try
            {
                Utility.printLog(query, verbose);
                using var cmd = new OracleCommand(query, con);
                using OracleDataReader dr = cmd.ExecuteReader();

                while (dr.Read())
                {
                    tableNames.Add(dr.GetString(0));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            return tableNames;
        }

        public List<String> getSchemaViews(String schemaName)
        {
            List<String> schemaNames = new List<String>();
            String query = "SELECT view_name FROM dba_views WHERE owner='" + schemaName + "' ORDER BY view_name";
            try
            {
                Utility.printLog(query, verbose);
                using var cmd = new OracleCommand(query, con);
                using OracleDataReader dr = cmd.ExecuteReader();

                while (dr.Read())
                {
                    schemaNames.Add(dr.GetString(0));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            return schemaNames;
        }

        public List<String> getSchemaMViews(String schemaName)
        {
            List<String> schemaNames = new List<String>();
            String query = "SELECT mview_name FROM dba_mviews WHERE owner='" + schemaName + "' ORDER BY mview_name";
            try
            {
                Utility.printLog(query, verbose);
                using var cmd = new OracleCommand(query, con);
                using OracleDataReader dr = cmd.ExecuteReader();

                while (dr.Read())
                {
                    schemaNames.Add(dr.GetString(0));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            return schemaNames;
        }

        public long getTableRowCount(String schemaName, String tableName)
        {
            long rowCount = 0;
            String query = "SELECT count(*) "
                    + "FROM " + schemaName + "." + tableName;
            try
            {
                using var cmd = new OracleCommand(query, con);
                rowCount = (long)Convert.ToDouble(cmd.ExecuteScalar().ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            return rowCount;
        }

        public int getTableIndexCount(String schemaName, String tableName)
        {
            int count = 0;
            String query = "SELECT count(*) "
                    + "FROM dba_indexes WHERE table_owner = '" + schemaName + "' AND table_name = '" + tableName + "'";
            try
            {
                using var cmd = new OracleCommand(query, con);
                count = (int)Convert.ToInt32(cmd.ExecuteScalar().ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            return count;
        }

        public void writeTablesInfo(List<String> schemaNames)
        {
            String fileName = "ora_tables.csv";
            try
            {
                if (File.Exists(fileName))
                {
                    File.Delete(fileName);
                }
                using StreamWriter fWriter = new StreamWriter(fileName, append: true);
                foreach (String schemaName in schemaNames)
                {
                    String schemaName2 = schemaName.ToUpper();
                    List<String> tableNames = getSchemaTables(schemaName2);
                    foreach (String tableName in tableNames)
                    {
                        long rowCount = getTableRowCount(schemaName2, tableName);
                        int indexCount = getTableIndexCount(schemaName2, tableName);
                        fWriter.Write(schemaName + "." + tableName.ToLower() + "," + rowCount + "," + indexCount + "\n");
                    }
                }
                Console.WriteLine("Oracle stats file (CSV) '" + fileName + "' created");
                fWriter.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        //TODO: combine all these similar methods as one method
        public void writeViewsInfo(List<String> schemaNames)
        {
            String fileName = "ora_views.csv";
            try
            {
                if (File.Exists(fileName))
                {
                    File.Delete(fileName);
                }
                using StreamWriter fWriter = new StreamWriter(fileName, append: true);
                foreach (String schemaName in schemaNames)
                {
                    String schemaName2 = schemaName.ToUpper();
                    List<String> viewNames = getSchemaViews(schemaName2);
                    foreach (String viewName in viewNames)
                    {
                        long rowCount = getTableRowCount(schemaName2, viewName);
                        fWriter.Write(schemaName + "." + viewName.ToLower() + "," + rowCount + "\n");
                    }
                }
                Console.WriteLine("Oracle stats file (CSV) '" + fileName + "' created");
                fWriter.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        //TODO: combine all these similar methods as one method
        public void writeMViewsInfo(List<String> schemaNames)
        {
            String fileName = "ora_mviews.csv";
            try
            {
                if (File.Exists(fileName))
                {
                    File.Delete(fileName);
                }
                using StreamWriter fWriter = new StreamWriter(fileName, append: true);
                foreach (String schemaName in schemaNames)
                {
                    String schemaName2 = schemaName.ToUpper();
                    List<String> viewNames = getSchemaMViews(schemaName2);
                    foreach (String viewName in viewNames)
                    {
                        long rowCount = getTableRowCount(schemaName2, viewName);
                        int indexCount = getTableIndexCount(schemaName2, viewName);
                        fWriter.Write(schemaName + "." + viewName.ToLower() + "," + rowCount + "," + indexCount + "\n");
                    }
                }
                Console.WriteLine("Oracle stats file (CSV) '" + fileName + "' created");
                fWriter.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }

        }
    }
}