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
using NLog.Fluent;

namespace WindowsService1
{
    public partial class Service1 : ServiceBase
    {
        private static Logger log = LogManager.GetCurrentClassLogger();
        private System.Timers.Timer _timer;
        private readonly string _excelFilePath = ConfigurationManager.AppSettings["ExcelFilePath"];
        private readonly string _connectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private DateTime _lastModifiedTime;
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1,1);
        public Service1()
        {
            InitializeComponent();
            _timer = new System.Timers.Timer();
            _timer.Interval = 6 * 1000;
            _timer.Elapsed += OnElapsedTime;
        }

        protected override void OnStart(string[] args)
        {
            log.Debug("Service started.");
            _ = SyncExcelToDatabaseAsync();
            _lastModifiedTime = File.GetLastWriteTime(_excelFilePath);
            _timer.Start();
        }

        protected override void OnStop()
        {
            log.Trace("Service stoped.");
            _timer.Stop();
        }

        private async void OnElapsedTime(object sender, ElapsedEventArgs e) 
        {
            await _semaphore.WaitAsync();
            try
            {
                
                    DateTime lastWriteTime = File.GetLastWriteTime(_excelFilePath);
                    if (lastWriteTime != _lastModifiedTime)
                    {
                        _lastModifiedTime = lastWriteTime;
                        await SyncExcelToDatabaseAsync();
                    }
                
            }
            catch (Exception ex)
            {
                log.Info(ex,"Error in OnElapsedTime.");
                throw;
            }
            finally { _semaphore.Release(); }
            
        
        }

        private  async Task SyncExcelToDatabaseAsync()
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
                            await DeleteRedundantDbData(dataTable, dbData);
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

        private IEnumerable<DataTable> SplitDataTable(DataTable dataTable, int chunkSize) {
            log.Warn("split the datatable.");
            for (int i = 1; i < dataTable.Rows.Count; i+= chunkSize)
            {
                var chunk = dataTable.Clone();
                for (int j = i; j < dataTable.Rows.Count && j < i + chunkSize; j++)
                {
                    chunk.ImportRow(dataTable.Rows[j]);
                }
                yield return chunk;
            }
        }
        private  async Task DeleteRedundantDbData(DataTable excelData, DataTable dbData)
        {
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
                                await DeleteRowsFromDatabaseAsync(connection, transaction, missingInExcel);
                                transaction.Commit();
                                log.Info("Missing rows deleted from db successfully!");
                            }
                            catch (Exception ex)
                            {
                                transaction.Rollback();
                                log.Error(ex, "Transaction rolled back due to error");

                            }
                        }
                    }
                }
            }
            catch (IOException ex)
            {
                log.Error(ex, "File access error");
            }
            catch (Exception ex)
            {
                log.Error(ex, "Error in SyncExcelToDatabaseAsync");
            }
        }
        private  async Task<DataTable> GetDatabaseDataAsync()
        {
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
        private static DataTable FindMissingInExcel(DataTable excelData, DataTable dbData)
        {
            DataTable missingInExcel = dbData.Clone();
            var excelEmails = excelData.AsEnumerable().Select(row => (string)row["Column4"]).ToHashSet();
            foreach (DataRow row in dbData.Rows)
            {
                string email = (string)row["Email"];
                if (!excelEmails.Contains(email))
                {
                    missingInExcel.ImportRow(row);
                }

            }
            return missingInExcel;
        }
        private static async Task DeleteRowsFromDatabaseAsync(SqlConnection connection, SqlTransaction transaction, DataTable rowsToDelete)
        {
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
