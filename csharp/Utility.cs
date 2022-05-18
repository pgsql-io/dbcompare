using System;
namespace OSCGP.DBCompare
{
    public class Utility
    {
        public Utility() {}
        static public string getCurrentTime()
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            return timestamp;
        }

        static public void printLog(String text, Boolean verbose)
        {
            if (verbose)
            {
                Console.WriteLine(getCurrentTime() + ": " + text);
            }
        }
    }
}
