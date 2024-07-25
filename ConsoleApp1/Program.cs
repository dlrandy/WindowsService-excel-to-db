using System.Data;
using ExcelDataReader;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using NLog;
using System.Text;
namespace ConsoleApp1
{
    public  class Program
    {
        private static Logger log = LogManager.GetCurrentClassLogger();
        private static readonly string _excelFilePath = @"C:\Users\邢斌斌\Desktop\test.xls";
        private static readonly string _connectionString = "Server=192.168.0.107;Database=UserExcel;User Id=sa;Password=MyS3cretP4$$;Integrated Security=False;MultipleActiveResultSets=True;TrustServerCertificate=True;";

       public static async Task Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            Console.WriteLine("Hello, World!");
            await SyncExcelToDatabaseAsync();
        }

        private static async Task SyncExcelToDatabaseAsync()
        {
            try
            {
                using (var stream = File.Open(_excelFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    using (var reader = ExcelReaderFactory.CreateReader(stream))
                    {
                        var result = reader.AsDataSet();
                        DataTable dataTable = result.Tables[0];
                        using (SqlConnection connection = new SqlConnection(_connectionString))
                        {
                            await connection.OpenAsync();
                            var dataChunks = SplitDataTable(dataTable, 1000);
                            var dbData = await GetDatabaseDataAsync();
                            await DeleteRedundantDbData(dataTable,dbData);
                            var tasks = dataChunks.Select(chunk => UpsertDataTableAsync(connection, chunk)).ToArray();
                            await Task.WhenAll(tasks);
                            log.Info("Excel data synchronized to database succefully.");
                        }
                    }
                }

            }
            catch (IOException ex)
            {
                log.Error(ex, "Error in SyncExcelToDatabase.");
                await Task.Delay(1000 * 10);
                await SyncExcelToDatabaseAsync();
            }
        }

        private static async Task UpsertDataTableAsync(SqlConnection connection, DataTable dataTable)
        {

            var upsertQueries = new List<string>();
            foreach (DataRow row in dataTable.Rows)
            {
                string upsertQuery = $@"
                    IF Exists (Select 1 from users where Email = '{row["Column4"]}')
                    Begin
                        update users
                        set Name = '{row["Column0"]}', Sex = {row["Column1"]},Dept = '{row["Column2"]}',Position = '{row["Column3"]}',EnrollDate = '{row["Column5"]}'
                        where Email = '{row["Column4"]}'
                    End
                    Else
                    Begin 
                        insert into users (Name,Sex,Dept,Position,Email,EnrollDate)
                        values('{row["Column0"]}',{row["Column1"]},'{row["Column2"]}','{row["Column3"]}','{row["Column4"]}','{row["Column5"]}')
                    End
        

                    ";
                upsertQueries.Add(upsertQuery);
            }
            var queries = string.Join(";", upsertQueries);
            using (SqlCommand command = new SqlCommand(queries, connection))
            {
                await command.ExecuteNonQueryAsync();
            }
        }
        private static IEnumerable<DataTable> SplitDataTable(DataTable dataTable, int chunkSize)
        {

            for (int i = 1; i < dataTable.Rows.Count; i += chunkSize)
            {
                var chunk = dataTable.Clone();
                for (int j = i; j < dataTable.Rows.Count && j < i + chunkSize; j++)
                {
                    chunk.ImportRow(dataTable.Rows[j]);
                }
                yield return chunk;
            }
        }

        private static async Task DeleteRedundantDbData(DataTable excelData,DataTable dbData) {
            try
            {
                var missingInExcel = FindMissingInExcel(excelData, dbData);
                if (missingInExcel.Rows.Count > 0)
                {
                    using (SqlConnection connection = new SqlConnection(_connectionString))
                    {
                        await connection.OpenAsync();
                        using (SqlTransaction transaction = connection.BeginTransaction())
                        {
                            try
                            {
                                await DeleteRowsFromDatabaseAsync(connection, transaction,missingInExcel);
                                transaction.Commit();
                                log.Info("Missing rows deleted from db successfully!");
                            }
                            catch (Exception ex)
                            {
                                transaction.Rollback();
                                log.Error(ex,"Transaction rolled back due to error");
                                
                            }
                        }
                    }
                }
            }
            catch (IOException ex)
            {
                log.Error(ex,"File access error");
            }
            catch (Exception ex){
            log.Error(ex,"Error in SyncExcelToDatabaseAsync");
            }
        }
        private static async Task<DataTable> GetDatabaseDataAsync() {
            DataTable dbData = new DataTable();
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                string query = "Select email from users";
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        dbData.Load(reader);
                    }
                }
            }
            return dbData;
        }
        private static DataTable FindMissingInExcel(DataTable excelData, DataTable dbData) {
            DataTable missingInExcel = dbData.Clone();
            var excelEmails = excelData.AsEnumerable().Select(row => (string)row["Column4"]).ToHashSet();
            foreach (DataRow row in dbData.Rows) {
                string email = (string)row["Email"];
                if (!excelEmails.Contains(email)) {
                    missingInExcel.ImportRow(row);
                }
                
            }
            return missingInExcel;
        }
        private static async Task DeleteRowsFromDatabaseAsync(SqlConnection connection, SqlTransaction transaction,DataTable rowsToDelete) {
            try
            {
                var emailsToDelete = rowsToDelete.AsEnumerable().Select(row => (string)row["Email"]).ToList();
                int batchSize = 100;
                for (int i = 0; i < emailsToDelete.Count; i += batchSize)
                {
                    var emails = emailsToDelete.Skip(i).Take(batchSize);
                    string batchEmails = string.Join("','", emails);
                    string deleteQuery = $"Delete from users where Email IN ('{batchEmails}')";
                    using (SqlCommand deleteCommand = new SqlCommand(deleteQuery, connection, transaction))
                    {
                        await deleteCommand.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {

                log.Error(ex);
            }
           
        
        }
    
    }
}
