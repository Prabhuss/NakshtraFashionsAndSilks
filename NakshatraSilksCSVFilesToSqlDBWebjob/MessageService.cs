using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;


namespace NakshatraSilksCSVFilesToSqlDBWebjob
{
    class MessageService
    {
        public static void SMSService(List<string> EventLogList)
        {
            try
            {
                var connection = ConfigurationManager.ConnectionStrings["MyConnection"].ConnectionString;
                var PhoneNumberOfCustomer = ConfigurationManager.AppSettings["PhoneNumberOfCustomer"];
                var NameOfCustomer = ConfigurationManager.AppSettings["NameOfCustomer"];
                Dictionary<string, string> smsConfig = new Dictionary<string, string>();
                List<string> BillModifiedLogList = new List<string>();
                //lets check for modified bills
                SplitListToarray(EventLogList, BillModifiedLogList);


                if (BillModifiedLogList.Count != 0)
                {
                    getSmsSetting(smsConfig, connection);
                    Console.WriteLine("sending the bill edited msgs\n");

                    foreach (string bill in BillModifiedLogList)
                    {
                        SendSMSToCustomerWithText(PhoneNumberOfCustomer, NameOfCustomer, bill, smsConfig);
                       
                    }
                       
                }
                else
                {
                    Console.WriteLine("No modified billls!!!!!");
                }

            }

            catch (Exception ex)
            {
                Console.WriteLine("{0}", ex.Message);
            }
        }

        private static List<string> SplitListToarray(List<string> eventModifiedList, List<string> BillModifiedLogList)
        {

            foreach (string lst in eventModifiedList)
            {
                string[] array = lst.Replace(" ", "").Split('L', '(', '-', ':', ')');
                var result = (array.Count() - array.Distinct().Count()) > 0;
                if (result == false)
                {
                    string textSMS = "The bill no " + array[1] + " has been modified. The old amount was " + array[4] + " and the new edited amount is " + array[6] + "\n" + "Nakshatra Silks";
                    BillModifiedLogList.Add(textSMS);
                }

            }

            return BillModifiedLogList;
        }

        public static void getSmsSetting(Dictionary<string, string> smsConfig, string connectionString)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand("select SettingName,SettingValue from CitrineSMSForTask", connection);
                SqlDataReader rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    smsConfig.Add(rdr[0].ToString().Trim(), rdr[1].ToString().Trim());
                }
                Console.WriteLine("the string dictionary is loaded with SettingNames and SettingValues of CitrineSMSForTask Table\n");
            }

        }




        public static void SendSMSToCustomerWithText(string phonenumber, string custName, string smstext, Dictionary<string, string> smsConfig)
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
