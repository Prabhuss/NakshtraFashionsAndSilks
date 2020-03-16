using System;
using Microsoft.Azure.WebJobs;

namespace NakshatraSilksCSVFilesToSqlDBWebjob
{
    
    class Program
    {
        static void Main()
        {
            JobHostConfiguration config = new JobHostConfiguration();
            config.Queues.MaxPollingInterval = TimeSpan.FromSeconds(1);
            config.Queues.MaxDequeueCount = 1;
            config.Queues.BatchSize = 1;
            var host = new JobHost(config);
            host.RunAndBlock();

        }
    }
}
