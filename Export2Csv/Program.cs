using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;

namespace Export2Csv
{
    public class AcquisitionConfig
    {
        public string id;
        public string type;
        public string table;
        public string connectionString;

        public AcquisitionConfig(string id, string type, string tableName,string connString)
        {
            this.id = id;
            this.type = type;
            this.table = tableName;
            this.connectionString = connString;
        }

    }

    class Program
    {
        static void Main(string[] args)
        {
            string configName = "";
            if(args.Length < 1)
            {
                Console.WriteLine("Add Sqlite or SQL after file name First!");
                return;
            }
            else
            {
                if(args[0] == "SQL")
                {
                    configName = "configSQL.csv";
                }else if(args[0] == "Sqlite")
                {
                    configName = "configSqlite.csv";
                }
                else
                {
                    Console.WriteLine("Add Sqlite or SQL after file name First!");
                    return;
                }
            }
            List<AcquisitionConfig> configs = new List<AcquisitionConfig>();
            Initialize(configs,configName);
            //Console.WriteLine(args[0]);
            //Console.WriteLine(args[1]);

            if (args[0] == "SQL")
            {
                foreach (AcquisitionConfig config in configs)
                {
                    ExportSqlServer2Csv(config);
                }
            }
            else
            {
                foreach (AcquisitionConfig config in configs)
                {
                    ExportSqlite2Csv(config);
                }
            }

            
            Console.WriteLine("Press any key to exit");
            Console.ReadKey();
        }

        static void Initialize(List<AcquisitionConfig> list ,string configName)
        {
            StreamReader sr = new StreamReader(configName, Encoding.UTF8);
            string line;
            string connectionString="";
            int i = 0;
            char[] chs = { ',' };
            while ((line = sr.ReadLine()) != null)
            {
                if (i == 0)
                {
                    connectionString = line;
                    i++;
                }
                else
                {
                    string[] items = line.Split(chs);
                    AcquisitionConfig config = new AcquisitionConfig( items[0], items[1], items[2],connectionString);
                    list.Add(config);
                }
            }
            sr.Close();
        }

        static void ExportSqlServer2Csv(AcquisitionConfig config)
        {
            string fileName = config.id + "-" + config.type+".csv";
            //string connectionString = "Data Source = " + textBoxIP.Text + ";Network Library = DBMSSOCN;Initial Catalog = " + textBoxDatabase.Text + ";User ID = " + textBoxUser.Text + ";Password = " + textBoxPwd.Text;
            string str="Data Source = 192.168.100.153;Network Library = DBMSSOCN;Initial Catalog = BridgeMonitoring;User ID = bridge_user;Password = 123456";
            string sqlStatement = "select Stamp,Value from "+config.table+" where SensorId='" + config.id + "' and Type='" + config.type + "' order by Stamp asc";
            StreamWriter sw = new StreamWriter(fileName, true);  //true表示如果a.txt文件已存在，则以追加的方式写入
            using (SqlConnection connection = new SqlConnection(config.connectionString))
            {
                connection.Open();

                SqlCommand command = new SqlCommand(sqlStatement, connection);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string stamp = reader.GetDateTime(0).ToString();
                        string value = reader.GetDouble(1).ToString();
                        string record = stamp + "," + value;
                        sw.WriteLine(record);
                    }
                    reader.Close();
                }
                connection.Close();
            }
        }

        static void ExportSqlite2Csv(AcquisitionConfig config)
        {
            string fileName = config.id + "-" + config.type + ".csv";
            string sqlStatement = "select Stamp,Value from " + config.table + " where SensorId='" + config.id + "' and Type='" + config.type + "' order by Stamp asc";
            StreamWriter sw = new StreamWriter(fileName, true);  //true表示如果a.txt文件已存在，则以追加的方式写入

            SQLiteConnection conn = null;

            int i = 0;
            try
            {
                conn = new SQLiteConnection(config.connectionString);
                conn.Open();
                SQLiteCommand command = new SQLiteCommand(sqlStatement, conn);
                SQLiteDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    string stamp = reader.GetString(0);
                    string value = reader.GetFloat(1).ToString();

                    string record = stamp + "," + value ;
                    sw.WriteLine(record);
                }
                conn.Close();
                sw.Close();
            }
            catch (Exception ex)
            {
                sw.Close();
                conn.Close();
                Console.WriteLine(ex.Message);
            }

        }
    }
}
