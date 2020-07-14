using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;


namespace NakshatraSilks_TotalSalesReport
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var connectionString = ConfigurationManager.ConnectionStrings["DBConnection"].ConnectionString;
                var phonenumbers = ConfigurationManager.AppSettings["PhoneNumberOfCustomer"];
                string[] phonenumber = phonenumbers.Split(',');

                Dictionary<string, string> BillAmountsModifiedDict = new Dictionary<string, string>();
                var BillAmounts = new Dictionary<string, string>();
                Dictionary<string, string> FinalBillAmounts = new Dictionary<string, string>();
                Dictionary<string, string> smsConfig = new Dictionary<string, string>();

                //initializing the sql connection to execute commands
                using (SqlConnection Connection = new SqlConnection(connectionString))
                {
                    Connection.Open();
                    //check if eventLog table contains any modified amounts 
                    EventModified(Connection, BillAmountsModifiedDict);

                    //sql query to get the bill nos and their amounts  from Taskinvmast Table
                    using (SqlCommand cmd = new SqlCommand("SELECT billno,nettotal FROM NakshatraSilks_invmast WHERE(CONVERT(varchar(10), CONVERT(date, invdate, 103), 120)) = cast(getdate()  as date) order by billno", Connection))
                    {
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                //loading the bill nos and amounts to a dictionary
                                BillAmounts[(string)reader["billno"]] = (string)reader["nettotal"];
                            }
                        }
                        Console.WriteLine("dictionary is loaded with all the bills and their corresponding amounts from NakshatraSilks_invmast Table\n");
                    }
                    //lets compare both the dictionaries and fetch correct values for all the bills 

                    decimal Total = ProcessingDictonaries(BillAmounts, BillAmountsModifiedDict);
                    //check if there are bills in invoicereturn table 
                    Decimal TotalReturnValue = CheckInvoiceReturnTable(Connection);
                    decimal FinalSales = Total - TotalReturnValue;

                    //lets check the number of customers who have visited the storewhen this webjob runs

                    int NumberOfCustomers = CountingNumberOfCustomers(Connection);
                    //printing the final sales value 


                    //adding 5 and a half hours to datetime as this webjob will be deployed 5 and a half hours earlier
                    DateTime currentTime = DateTime.Now;
                    DateTime x5hrsLater = currentTime.AddHours(5);
                    DateTime x30MinsLater = x5hrsLater.AddMinutes(30);

                    string SMSText = "Todays Total Sales Report is " + FinalSales + " INR  And the number of Customers who have visited the store by " + x30MinsLater + " are " + NumberOfCustomers + "\n Nakshatra Silks";

                    //lets get the settings value to send the message now!!!
                    getSmsSetting(smsConfig, Connection);

                    if (x30MinsLater.Hour == 20)
                    {
                        SendSMSToCustomerWithText(phonenumber[0], SMSText, smsConfig);
                    }

                    else
                    {
                        foreach (string num in phonenumber)
                            SendSMSToCustomerWithText(num, SMSText, smsConfig);

                    }



                }





            }
            catch (Exception ex)
            {
                Console.WriteLine("\n{0}\n", ex.Message);
            }




        }

        private static int CountingNumberOfCustomers(SqlConnection connection)
        {
            using (SqlCommand cmd = new SqlCommand("select count(billno) from NakshatraSilks_invmast WHERE(CONVERT(varchar(10), CONVERT(date, invdate, 103), 120)) = cast(getdate()  as date) ", connection))
            {

                int NumberOfCustomers = (int)cmd.ExecuteScalar();
                return NumberOfCustomers;
            }

        }
        public static Dictionary<string, string> EventModified(SqlConnection connection, Dictionary<string, string> BillAmountsModifiedDict)
        {
            try
            {
                List<string> EventModifiedList = new List<string>();
                using (SqlCommand cmd = new SqlCommand("select event from NakshatraSilks_eventLog WHERE(CONVERT(varchar(10), CONVERT(date, edate, 103), 120)) = cast(getdate()  as date)and event Like '%Modify Sales%'", connection))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            EventModifiedList.Add(reader["event"].ToString());
                        }
                        if (reader != null)
                            Console.WriteLine("The EventModifiedList is loaded with all the Modify Sales Voucher events\n");
                        else
                            Console.WriteLine("There are no Modify Sales Voucher events\n");
                    }

                }

                Console.WriteLine("checking if EventModifiedList has old and new edited amounts of bills......\n.");
                SplitListToarray(EventModifiedList, BillAmountsModifiedDict);
                return BillAmountsModifiedDict;
            }
            catch (Exception ex)
            {
                Dictionary<string, string> Empty = new Dictionary<string, string>();
                Console.WriteLine("failed to fetch eventLog modified details\n{0}", ex.Message);
                return Empty;
            }
        }

        public static string SplitListToarray(List<string> eventModifiedList, Dictionary<string, string> BillAmountsModifiedList)
        {
            foreach (string lst in eventModifiedList)
            {
                string[] array = lst.Replace(" ", "").Split('L', '(', '-', ':', ')');
                var result = (array.Count() - array.Distinct().Count()) > 0;
                if (result == false)
                {
                    BillAmountsModifiedList[array[1]] = array[6];
                }

            }
            if (BillAmountsModifiedList.Count > 0)
            {
                Console.WriteLine("BillAmountsModifiedList has a few bill with  modified amounts\n");
                return BillAmountsModifiedList.ToString();
            }
            else
                Console.WriteLine("there exists no bills with modified amounts\n");
            return "there exists no bills with modified amounts";




        }

        private static Decimal ProcessingDictonaries(Dictionary<string, string> BillAmounts, Dictionary<string, string> BillAmountsModifiedDict)
        {
            try
            {
                var merged = BillAmountsModifiedDict.Concat(BillAmounts).ToLookup(x => x.Key, x => x.Value).ToDictionary(x => x.Key, g => g.First());
                var TotalSales = merged.Sum(x => Convert.ToDecimal(x.Value));
                Console.WriteLine("total sales report as per chcking modified bills - {0} INR", TotalSales);
                return TotalSales;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return 0;
            }


        }

        private static Decimal CheckInvoiceReturnTable(SqlConnection Connection)
        {
            try
            {
                List<string> InvoiceReturnBillsList = new List<string>();
                using (SqlCommand cmd = new SqlCommand("select net from NakshatraSilks_invoicereturn  WHERE(CONVERT(varchar(10), CONVERT(date, date, 103), 120))= cast(getdate() as date) ", Connection))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            InvoiceReturnBillsList.Add(reader["net"].ToString());
                        }
                        if (reader != null)
                            Console.WriteLine("The InvoiceReturnBillsList is loaded with all return bill amounts\n");
                        else
                            Console.WriteLine("There are no returned bills \n");
                    }

                }
                Decimal totalReturn = InvoiceReturnBillsList.Sum(x => Convert.ToDecimal(x));
                return totalReturn;
            }
            catch (Exception ex)
            {
                Console.WriteLine("failed to fetch the return bills!!!!!!!-{0}", ex.Message);
                return 0;
            }

        }




        public static Dictionary<string, string> getSmsSetting(Dictionary<string, string> smsConfig, SqlConnection connectionString)
        {
            try
            {
                //SqlCommand cmd = new SqlCommand("select SettingName,SettingValue from CitrineSMSForTask Where MerchantBranchId="+"'"+ConfigurationManager.AppSettings["MerchantBranchId"]+"'", connectionString);
                SqlCommand cmd = new SqlCommand("select SettingName,SettingValue from CitrineSMSForTask Where MerchantBranchId=179", connectionString);
                SqlDataReader rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    smsConfig.Add(rdr[0].ToString().Trim(), rdr[1].ToString().Trim());
                }
                Console.WriteLine("the string dictionary is loaded with SettingNames and SettingValues of CitrineSMSForTask Table\n");
            }
            catch (Exception e)
            {
                Console.WriteLine("Error" + e.Message);
            }
            

            return smsConfig;

        }


        public static void SendSMSToCustomerWithText(string phonenumber, string smstext, Dictionary<string, string> smsConfig)
        {
            try
            {
                string apiKey = smsConfig["SMS_API_KEY"];
                string countryCode = (smsConfig["SMS_COUNTRY_CODE"] == "" || smsConfig["SMS_COUNTRY_CODE"] == null) ? "91" : smsConfig["SMS_COUNTRY_CODE"];
                string merchantSenderId = smsConfig["SMS_SENDER_ID"];
                string routeVal = smsConfig["SMS_ROUTE_VAL"];

                if ((phonenumber != "") && (phonenumber != ":"))
                {
                    string msg;
                    string httplinkstr = "https://control.msg91.com/api/sendhttp.php?";
                    string authkey = "authkey=" + apiKey + "&";
                    string country = "country" + countryCode + "&";
                    phonenumber = "mobiles=" + phonenumber + "&";
                    msg = "message=" + smstext + "&";
                    string senderId = "sender=" + merchantSenderId + "&";
                    string route = "route=" + routeVal;
                    string smsMsg = httplinkstr + authkey + phonenumber + country + msg + senderId + route;
                    Console.WriteLine(smsMsg);
                    SendSMSToURL(smsMsg);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }


        }
        public static string SendSMSToURL(string uri)
        {
            try
            {
                string SentResult = String.Empty;
                string StatusCode = String.Empty;

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(uri);

                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                StreamReader responseReader = new StreamReader(response.GetResponseStream());

                String resultmsg = responseReader.ReadToEnd();
                responseReader.Close();

                int StartIndex = 0;
                int LastIndex = resultmsg.Length;

                if (LastIndex > 0)
                    SentResult = resultmsg.Substring(StartIndex, LastIndex);

                HttpStatusCode objHSC = response.StatusCode;
                responseReader.Dispose();
                Console.WriteLine("SMS looks good");
                Console.WriteLine(resultmsg);
                return SentResult;
            }
            catch (Exception)
            {
                Console.WriteLine("SMS Sent is not sucessful");
                return "bad";
            }

        }
    }
}




