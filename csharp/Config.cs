using System;
using System.Configuration;

namespace OSCGP.DBCompare
{
    public class Config
    {
        public Config()
        {
        }

        public static String getProperty(string key)
        {
            string result = key + ", Not Found";
            try
            {
                var appSettings = ConfigurationManager.AppSettings;
                result = appSettings[key] ?? key + ", Not Found";
            }
            catch (ConfigurationErrorsException)
            {
                Console.WriteLine("Error reading app settings");
            }
            return result;
        }
    }
}
