using System;
using System.Collections.Generic;
using System.IO;
using Npgsql;

namespace OSCGP.DBCompare
{
    public class PGConnect
    {
        NpgsqlConnection con;
        Boolean verbose = false;
        List <String> schemaNames = null;

        public PGConnect(Boolean verbose)
        {
            this.verbose = verbose;
        }

        public void setSchemaNames(List<String> schemaNames)
        {
            this.schemaNames = schemaNames;
        }

        public void run()
        {
            connectPG(Config.getProperty("pg.connect"), Config.getProperty("pg.user"),
                Config.getProperty("pg.password"));
            writeTablesInfo(schemaNames);
            writeViewsInfo(schemaNames);
            writeMViewsInfo(schemaNames);
        }

        public Boolean connectPG(String connectionString, String user, String password)
        {
            con = new NpgsqlConnection(connectionString + ";Username=" + user + ";Password=" + password);
            con.Open();
            Console.WriteLine("Opened PostgreSQL database successfully");
            Console.WriteLine("PostgreSQL Database version : " + con.PostgreSqlVersion);
            return true;
        }

        public List<String> getSchemaTables(String schemaName)
        {
            List<String> tableNames = new List<String>();
            String query = "SELECT c.relname AS \"tablename\" "
                    + "FROM   pg_catalog.pg_class c "
                    + "JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE  c.relkind = ANY ('{p,r,\"\"}') "
                    + "AND    NOT c.relispartition "
                    + "AND    n.nspname = '" + schemaName + "' "
                    + "AND    c.relname NOT LIKE '%$%' "
                    + "ORDER  BY 1;";
            try
            {
                Utility.printLog(query, verbose);
                using var cmd = new NpgsqlCommand(query, con);
                using NpgsqlDataReader rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    tableNames.Add(rdr.GetString(0));
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
            String query = "SELECT viewname FROM pg_catalog.pg_views " +
                           "WHERE schemaname = '" + schemaName + "' " +
                           "ORDER BY 1;";
            try
            {
                Utility.printLog(query, verbose);
                using var cmd = new NpgsqlCommand(query, con);
                using NpgsqlDataReader rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    schemaNames.Add(rdr.GetString(0));
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
            String query = "SELECT matviewname FROM pg_catalog.pg_matviews " +
                           "WHERE schemaname = '" + schemaName + "' " +
                           "ORDER BY 1;";
            try
            {
                Utility.printLog(query, verbose);
                using var cmd = new NpgsqlCommand(query, con);
                using NpgsqlDataReader rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    schemaNames.Add(rdr.GetString(0));
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
                        + "FROM " + schemaName + "." + tableName + ";";
            try
            {
                using var cmd = new NpgsqlCommand(query, con);
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
                        + "FROM pg_indexes WHERE schemaname = '" + schemaName + "' AND tablename = '" + tableName + "';";
            try
            {
                using var cmd = new NpgsqlCommand(query, con);
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
            String pgFileName = "pg_tables.csv";
            try
            {
                if (File.Exists(pgFileName))
                {
                    File.Delete(pgFileName);
                }
                using StreamWriter fWriter = new StreamWriter(pgFileName, append: true);
                foreach (String schemaName in schemaNames)
                {
                    List<String> tableNames = getSchemaTables(schemaName);
                    foreach (String tableName in tableNames)
                    {
                        long rowCount = getTableRowCount(schemaName, tableName);
                        int indexCount = getTableIndexCount(schemaName, tableName);
                        fWriter.Write(schemaName + "." + tableName + "," + rowCount + "," + indexCount + "\n");
                    }
                }
                Console.WriteLine("PostgreSQL stats file (CSV) '" + pgFileName + "' created");
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
            String pgFileName = "pg_views.csv";
            try
            {
                if (File.Exists(pgFileName))
                {
                    File.Delete(pgFileName);
                }
                using StreamWriter fWriter = new StreamWriter(pgFileName, append: true);
                foreach (String schemaName in schemaNames)
                {
                    List<String> viewNames = getSchemaViews(schemaName);
                    foreach (String viewName in viewNames)
                    {
                        long rowCount = getTableRowCount(schemaName, viewName);
                        fWriter.Write(schemaName + "." + viewName + "," + rowCount + "\n");
                    }
                }
                Console.WriteLine("PostgreSQL stats file (CSV) '" + pgFileName + "' created");
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
            String pgFileName = "pg_mviews.csv";
            try
            {
                if (File.Exists(pgFileName))
                {
                    File.Delete(pgFileName);
                }
                using StreamWriter fWriter = new StreamWriter(pgFileName, append: true);
                foreach (String schemaName in schemaNames)
                {
                    List<String> viewNames = getSchemaMViews(schemaName);
                    foreach (String viewName in viewNames)
                    {
                        long rowCount = getTableRowCount(schemaName, viewName);
                        int indexCount = getTableIndexCount(schemaName, viewName);
                        fWriter.Write(schemaName + "." + viewName + "," + rowCount + "," + indexCount + "\n");
                    }
                }
                Console.WriteLine("PostgreSQL stats file (CSV) '" + pgFileName + "' created");
                fWriter.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }

        }
    }
}