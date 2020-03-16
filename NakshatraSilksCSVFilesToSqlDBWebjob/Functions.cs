using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using Microsoft.Azure.WebJobs;

namespace NakshatraSilksCSVFilesToSqlDBWebjob
{
    public class Functions
    {

        static int GettWordAfteKey(string[] array, string keyvalue)
        {
            for (int i = 0; i < array.Length; i++)
            {

                if (array[i] == keyvalue)
                {
                    if (keyvalue == "KEY1")
                    {
                        if (array[i + 1] == "eventLog")
                            return i + 1;
                        else
                            return i + 2;
                    }
                    else
                        return i + 1;
                }
            }
            return -1;
        }

        public static void ProcessQueueMessage([QueueTrigger("merchantid-179")] string blobName, [Blob("merchantid-179/{queueTrigger}")] Microsoft.WindowsAzure.Storage.Blob.CloudBlockBlob blobToDownload)
        {

            Console.WriteLine("Msg is {0}", blobName);
            string[] words = blobName.Split('_');
            MemoryStream ms;

            try
            {
                ms = new MemoryStream();
                blobToDownload.DownloadToStream(ms);
            }

            catch (Exception e)
            {
                Console.WriteLine("Blob Download to memory has Failed for Blob Name " + blobName + e.Message);
                return;
            }


            int key4Index = GettWordAfteKey(words, "KEY4");
            int key5Index = GettWordAfteKey(words, "KEY5");
            int key1Index = GettWordAfteKey(words, "KEY1");

            string storeid;
            string posid;
            string tablename;

            if (key4Index != -1)
            {
                Console.WriteLine(" store id is  = " + words[key4Index]);
                storeid = (words[key4Index]);
            }
            else
            {
                Console.WriteLine(" store id is  -1");
                return;
            }
            if (key5Index != -1)
            {
                Console.WriteLine(" pos id is  = " + words[key5Index]);
                posid = (words[key5Index]);
            }
            else
            {
                Console.WriteLine(" pos id is  -1");
                return;
            }
            if (key1Index != -1)
            {
                Console.WriteLine(" table name is  = " + words[key1Index]);
                tablename = (words[key1Index]);
            }
            else
            {
                Console.WriteLine("table name  is  -1");
                return;
            }
            try
            {
                if (blobName.Contains(".csv"))
                    ProcesssingAllTable(tablename, ms, storeid, posid, blobName);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        public static void ProcesssingAllTable(string tablename, MemoryStream ms, string storeid, string posid, string blobName)
        {
            try
            {

                //loading the contents of memory to a dynamic array
                string text = Encoding.UTF8.GetString(ms.ToArray());
                ConvertStringToDataTable(tablename, text, storeid, posid, blobName);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }


        }


        public static void ConvertStringToDataTable(string tablename, string text, string storeid, string posid, string blobName)
        {
            try
            {
                DataTable dataTable = new DataTable();
                // extract all lines:
                string[] lines = text.Split(new string[] { "\r\n", "\n" }, StringSplitOptions.RemoveEmptyEntries);
                var header = lines[0];
                // first create the columns:
                string[] columns = header.Split(','); // using commas as delimiter is brave 
                dataTable.Columns.Add("storeid"); //add storeid as a column name
                dataTable.Columns.Add("posid");  //add posid as a column 
                foreach (string col in columns)
                    dataTable.Columns.Add(col.Trim());//add all the columns of string text 
                dataTable.Columns.Add("InsertedDate"); //add  InsertedDate as a column name
                dataTable.Columns.Add("BlobName");//add BlobName as a column name 


                foreach (string line in lines)
                {
                    if (line == header)
                    {
                        continue;   //ignore the first line as it is the same column names
                    }
                    else
                    {
                        string[] fields = line.Split(','); //split each word of a line using commas
                        for (int j = 0; j < fields.Length; j++)
                        {
                            if (fields[j] == " ")
                            {
                                fields[j] = null;

                            }
                        }
                        //intialize the dataarow
                        DataRow dr1 = dataTable.NewRow();
                        //add the storeid and posid values in the data cells
                        dr1["storeid"] = storeid;
                        dr1["posid"] = posid;

                        //add the rest of the values from fields string 
                        int i = 0;
                        foreach (var c in columns)
                        {
                            if (i < fields.Length)
                            {
                                dr1[c] = fields[i];
                            }
                            i++;
                        }
                        dr1["InsertedDate"] = DateTime.Now;
                        dr1["blobName"] = blobName;
                        //itemArray combines the values of storeid,posid,and fields in same row
                        dataTable.Rows.Add(dr1.ItemArray);

                    }

                }
                if (dataTable != null)
                    InsertDataIntoSQLServer(dataTable, tablename);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }




        public static void InsertDataIntoSQLServer(DataTable csvFileData, string tablename)
        {
            try
            {  //read the credentials of datatbase
                var connectionstring = ConfigurationManager.ConnectionStrings["MyConnection"].ConnectionString;
                using (SqlConnection dbConnection = new SqlConnection(connectionstring))
                {
                    dbConnection.Open();
                    object Date = DateTime.Today.ToString("dd/MM/yyyy");

                    if (tablename == "eventLog")

                    {
                        foreach (DataRow dr in csvFileData.Select("event like '%Modify Sales%'  AND edate like '%" + Date + "%' "))

                        {
                            Console.WriteLine("This was" + tablename + "table . Lets check if there are modified records!!!!");
                            List<string> EventLogList = new List<string>();
                            EventLogList.Add(dr["event"].ToString());
                            MessageService.SMSService(EventLogList);
                        }
                    }

                    using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                    {
                        s.DestinationTableName = "NakshatraSilks_" + tablename ;
                        s.BatchSize = 50;
                        bool exists = TableCreationClass.TableExistsOrNot(dbConnection, s.DestinationTableName);
                        if (!exists)
                        {
                            Console.WriteLine(tablename + ": this table is not in the database!!");
                            TableCreationClass.CreateNewTable(csvFileData, s.DestinationTableName, dbConnection);
                        }
                        else
                            Console.WriteLine(tablename + ": Table exists!!");
                        // Add your column mappings here
                        foreach (DataColumn column in csvFileData.Columns)
                        {
                            s.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                        }

                        // Finally write to server
                        s.WriteToServer(csvFileData);
                    }
                    //check for duplicates
                    DeleteDuplicateRecords(csvFileData, dbConnection, tablename);
                }
                Console.WriteLine("Done!!!!!The table looks clean Now");

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }




        }

        public static void DeleteDuplicateRecords(DataTable csvFileData, SqlConnection dbConnection, string tablename)
        {
            //query for deleting the duplicate records
            string SQL = " With delDuplicate As( SELECT*, ROW_NUMBER() OVER(PARTITION BY " + csvFileData.Columns[2] + " ORDER BY " +
                  csvFileData.Columns[2] + ")" +
                    " AS RowNumber FROM NakshatraSilks_" + tablename  + ")DELETE   FROM delDuplicate WHERE RowNumber > 1";
            SqlCommand cmd = new SqlCommand(SQL, dbConnection);
            // we are inserting updating records hence using executenonquery
            cmd.ExecuteNonQuery();
            //the command may take infinite time to execute
            cmd.CommandTimeout = 0;
            var rowsUpdated = cmd.ExecuteNonQuery();
            //if rows are updated duplicate records are deleted
            if (rowsUpdated > 0)
                Console.WriteLine("Duplicates are removed!!!!");
            else
                Console.WriteLine("no Duplicates!!!!");


        }

    }


}
