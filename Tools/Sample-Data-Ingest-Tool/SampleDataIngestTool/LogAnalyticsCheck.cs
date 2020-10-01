using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.Azure.OperationalInsights;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;

namespace SampleDataIngestTool
{
    public class LogAnalyticsCheck
    {
        public LogAnalyticsCheck()
        {

        }

        public bool RunLAQuery(string tableName)
        {
            try
            {
                string customerId = ConfigurationManager.AppSettings["workspaceId"];
                string clientId = ConfigurationManager.AppSettings["clientId"];
                string clientSecret = ConfigurationManager.AppSettings["clientSecret"];
                string domain = ConfigurationManager.AppSettings["domain"]; 

                var authEndpoint = "https://login.microsoftonline.com";
                var tokenAudience = "https://api.loganalytics.io/";

                var adSettings = new ActiveDirectoryServiceSettings
                {
                    AuthenticationEndpoint = new Uri(authEndpoint),
                    TokenAudience = new Uri(tokenAudience),
                    ValidateAuthority = true
                };

                var creds = ApplicationTokenProvider.LoginSilentAsync(domain, clientId, clientSecret, adSettings).GetAwaiter().GetResult();

                var laClient = new OperationalInsightsDataClient(creds);
                laClient.WorkspaceId = customerId;

                //get a list of table names in your workspace
                var tableNameList = new List<string>();
                string query = @"search * | distinct $table";
                var result = laClient.Query(query).Tables;
                foreach (var table in result)
                {
                    var rows = table.Rows;
                    foreach (var r in rows)
                    {
                        var customFileName = r[0];
                        if (customFileName.EndsWith("_CL"))
                        {
                            tableNameList.Add(customFileName);
                        }
                    }
                }

                //check if the custom table name exists in the list
                if (tableNameList.Contains(tableName) == false)
                {
                    return false;
                }
                else
                {
                    //check if there's any data in the table for last 7 days
                    string query1 = tableName
                               + @"| where TimeGenerated > ago(7d)
                             | limit 10";
                    var results = laClient.Query(query1);
                    var tableCount = results.Tables.Count;
                    if (tableCount > 0)
                    {
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Calling Log Analytics Error " + ex.Message);
            }
        }
    }
}