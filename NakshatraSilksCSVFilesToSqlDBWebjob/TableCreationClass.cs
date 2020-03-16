using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;


namespace NakshatraSilksCSVFilesToSqlDBWebjob
{
    class TableCreationClass
    {
        public static bool TableExistsOrNot(SqlConnection dbConnection, string tableName)
        {
            bool exists = false;
            DataTable dt = dbConnection.GetSchema("Tables");
            foreach (DataRow row in dt.Rows)
            {
                string tablename = (string)row[2];
                if (tablename == tableName)
                {
                    Console.WriteLine("{0}, {1}, {2}", row[0], row[1], row[2]);
                    exists = true;
                    return exists;
                }
            }
            return exists;
        }
        public static void CreateNewTable(DataTable csvFileData, string tableName, SqlConnection dbConnection)
        {
            string columnsStringForSql = "";
            string columnType = "nvarchar(250)";
            List<string> columnList;
            try
            {
                //printColumnDetails(csvFileData);
                columnList = FetchColumnList(csvFileData);
                int totalColumns = csvFileData.Columns.Count;
                int i = 0;
                foreach (string column in columnList)
                {
                    i++;
                    columnsStringForSql += column + "\t" + columnType;
                    if (i < totalColumns)
                    {
                        columnsStringForSql += " , ";
                    }

                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            string strSQL = "CREATE TABLE " + tableName + " (" + columnsStringForSql + " );";
            //Console.WriteLine(strSQL);
            try
            {
                using (SqlCommand command = new SqlCommand(strSQL, dbConnection))
                    command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
        static void printColumnDetails(DataTable schemaTable)
        {
            foreach (DataColumn column in schemaTable.Columns)
            {
                Console.Write(column.ColumnName);
            }
        }
        public static List<string> FetchColumnList(DataTable schemaTable)
        {
            List<string> columnList = new List<string>();
            try
            {
                foreach (DataColumn column in schemaTable.Columns)
                {
                    string columnTemp = column.ColumnName;
                    columnList.Add(columnTemp);
                }
                return columnList;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return columnList;
            }
        }
    }
}
