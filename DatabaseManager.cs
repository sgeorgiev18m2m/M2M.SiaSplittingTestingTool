using M2M.SiaSplittingTestingTool.Contracts;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace M2M.SiaSplittingTestingTool
{    public static class DatabaseManager
    {
        static IConfiguration _config;
        static string _connectionString;
        public static void DatabaseManagerAppsettingsConfiguration(IConfiguration config)
        {
            _config = config;
            _connectionString = _config.GetConnectionString("HomeAutomation")!;
        }
        public static List<SiaEvent> GetEventsFromFile(string path)
        {
            List<SiaEvent> siaList = new List<SiaEvent>();
            try
            {
                foreach (string line in File.ReadLines(path))
                {
                    siaList.Add(new SiaEvent(line));
                }
            }
            catch
            {

            }

            return siaList;
        }
        public static List<SiaEvent> GetTwoHundredThousandEvents(Int64? id = null)
        {
            List<SiaEvent> siaList = new List<SiaEvent>();

            try
            {
                string query = "SELECT TOP (200000) ID, Value " +
                                "FROM VariablesLog with (nolock) " +
                                "where [Value] LIKE '#%' ";

                if (id != null && id > 0)
                {
                    query += "AND ID < " + id + " ";
                }

                query += "order by id desc ";

                using SqlConnection conn = new SqlConnection(_connectionString);
                {
                    conn.Open();

                    SqlCommand command = new SqlCommand(query, conn);

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Int64 siaId = (Int64)reader["ID"];
                            string msg = (string)reader["Value"];

                            siaList.Add(new SiaEvent(siaId, msg));
                        }

                        reader.Close();
                    }
                }
            }
            catch (Exception ex)
            {

            }

            return siaList;
        }
    }
}
