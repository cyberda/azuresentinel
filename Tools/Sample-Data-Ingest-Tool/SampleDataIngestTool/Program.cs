using System;
using System.Configuration;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace SampleDataIngestTool
{
    class Program
    {
        // You can use an optional field to specify the timestamp from the data. If the time field is not specified, Azure Monitor assumes the time is the message ingestion time
        static string timeStampField = "";
        private static string customerId;
        private static string sharedKey;

        static void Main(string[] args)
        {
            string sampleDataPath = ConfigurationManager.AppSettings["sampleDataPath"];
            if (!Directory.Exists(sampleDataPath))
            {
                throw new IOException("Specified directory not exists");
            }

            string[] files = Directory.GetFiles(sampleDataPath, "*.json", SearchOption.AllDirectories);

            if (files.Length > 0)
            {
                customerId = ConfigurationManager.AppSettings["workspaceId"];
                sharedKey = ConfigurationManager.AppSettings["sharedKey"];
                 
                var laCheck = new LogAnalyticsCheck();            
                   
                // Loop through files 
                foreach (var file in files)
                {
                    string tableName = Path.GetFileName(file).Replace(".json", "");
                    // Check if the file has been pushed to the Log Analytics
                    bool result = laCheck.RunLAQuery(tableName);
                    if (result == true)
                    {
                        // Prompt user to choose to repush data
                        Console.WriteLine("{0} has been posted. Would you like to post it again?", tableName);
                        var res = Console.ReadLine();
                        if(res.ToLower() == "y" || res.ToLower() == "yes")
                        {
                            PushDataToLog(file);
                        }
                        else
                        {
                            Console.WriteLine("Check Log Analytics for existing data");
                        }
                    }
                    else
                    {
                        PushDataToLog(file);
                    }
                }
            }
            else
            {
                Console.WriteLine("No Custom files in Sample Data");
            }
        }

        //Build the API signature
        public static string BuildSignature(string message, string secret)
        {
            try
            {
                var encoding = new System.Text.ASCIIEncoding();
                byte[] keyByte = Convert.FromBase64String(secret);
                byte[] messageBytes = encoding.GetBytes(message);
                using (var hmacsha256 = new HMACSHA256(keyByte))
                {
                    byte[] hash = hmacsha256.ComputeHash(messageBytes);
                    return Convert.ToBase64String(hash);
                }
            }
            catch (Exception excep)
            {
                Console.WriteLine("Authenticaton Error: " + excep.Message);
                return "Authentication Error" + excep.Message;
            }

        }

        //Read each file in Sample Data repo
        public static string ReadFile(string filePath)
        {
            try
            {
                using (StreamReader streamReader = new StreamReader(filePath))
                {
                    var json = streamReader.ReadToEnd();

                    return json;
                }
            }
            catch (Exception excep)
            {
                Console.WriteLine("Read File exception: " + excep.Message);
                return "Read File error";
            }

        }

        //Send posted data to Log Analytics custom logs
        private static void PushDataToLog(string filePath)
        {
            try
            {
                //Create a hash for the API signature
                var datestring = DateTime.UtcNow.ToString("r");
                string json = ReadFile(filePath);
                var jsonBytes = Encoding.UTF8.GetBytes(json);
                string stringToHash = "POST\n" + jsonBytes.Length + "\napplication/json\n" + "x-ms-date:" + datestring + "\n/api/logs";
                string hashedString = BuildSignature(stringToHash, sharedKey);
                string signature = "SharedKey " + customerId + ":" + hashedString;
                PostData(signature, datestring, json, filePath);
            }
            catch (Exception excep)
            {
                Console.WriteLine("Error to push data: " + excep.Message);
            }
        }

        //Send a request to the POST API endpoint for Custom Log
        public static void PostData(string signature, string date, string json, string filePath)
        {
            try
            {
                string url = "https://" + customerId + ".ods.opinsights.azure.com/api/logs?api-version=2016-04-01";

                HttpClient client = new HttpClient();
                client.DefaultRequestHeaders.Add("Accept", "application/json");


                string logName = Path.GetFileName(filePath).Replace("_CL.json", "").Replace(".json", "");
                client.DefaultRequestHeaders.Add("Log-Type", logName);
                client.DefaultRequestHeaders.Add("Authorization", signature);
                client.DefaultRequestHeaders.Add("x-ms-date", date);
                client.DefaultRequestHeaders.Add("time-generated-field", timeStampField);

                HttpContent httpContent = new StringContent(json, Encoding.UTF8);
                httpContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                Task<HttpResponseMessage> response = client.PostAsync(new Uri(url), httpContent);
                HttpContent responseContent = response.Result.Content;
                string result = responseContent.ReadAsStringAsync().Result;

                var fileName = logName + "_CL.json";
                
                if (response.Result.StatusCode.ToString().Contains("OK"))
                {
                    Console.WriteLine("{0} is successfully pushed", fileName);
                }
                else
                {
                    Console.WriteLine("Failed to push {0}", fileName);
                }
            }
            catch (Exception excep)
            {
                Console.WriteLine("API Post Exception: " + excep.Message);
            }
        }
    }
}
