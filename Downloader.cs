#define Manual
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace Downloader
{
    class LogDownloader
    {
        public static void StartDownloader(DateTime starttime,String Dir)
        {
            var connectionString = ConfigurationManager.ConnectionStrings["StorageConnectionStringV3"].ConnectionString;

            var containerName = ConfigurationManager.AppSettings["ContainerName"].ToString();

            var directory = ConfigurationManager.AppSettings["BlobDirectory"].ToString();

            var dstDirectory = ConfigurationManager.AppSettings["DestDirectory"].ToString();

            int lastHour = -1;
#if Manual
            DateTime iter_end = DateTime.Now.AddHours(-1);
            DateTime iter_time = starttime;
            while (iter_time <= iter_end)
            {
                Download(connectionString, containerName, directory, iter_time, Dir);
                Console.WriteLine("[{0}] Download finish", iter_time);
                iter_time = iter_time.AddHours(1);
            }

#else
            while (true)
            {
                try
                {
                    var now = DateTime.Now;

                    if (now.Hour != lastHour && now.Minute > 10)
                    {
                        lastHour = now.Hour;

                        Console.WriteLine("[{0}] Begin download blob", DateTime.Now);

                        Download(connectionString, containerName, directory, now.AddHours(-1), dstDirectory);

                        Console.WriteLine("[{0}] Download finish", DateTime.Now);
                    }
                }
                catch (Exception exp)
                {
                    Console.WriteLine("Exception: " + exp.ToString());

                    Thread.Sleep(2 * 60 * 1000);
                }

                Thread.Sleep(60 * 1000);
            }
#endif
        }

        private static void Download(string connectionString, string containerName, string directory, DateTime time, string dstDirectory)
        {
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            //var utcTime = time.ToUniversalTime();
            var utcTime = time;

            var dirName = directory + "/" + String.Format("{0}/{1:D2}/{2:D2}/{3:D2}", utcTime.Year, utcTime.Month, utcTime.Day, utcTime.Hour);

            var dir = container.GetDirectoryReference(dirName);

            var list = dir.ListBlobs().ToList();

            // download one by one
            foreach (var item in list)
            {
                if (!(item is CloudBlockBlob))
                    continue;

                var blob = item as CloudBlockBlob;

                var stream = new MemoryStream();

                var text = blob.DownloadText();

                //var outputDirectory = Path.Combine()

                //item.StorageUri.PrimaryUri.

                var outputPath = Path.Combine(dstDirectory + item.StorageUri.PrimaryUri.LocalPath);

                var outputDirectory = Path.GetDirectoryName(outputPath);

                Directory.CreateDirectory(outputDirectory);

                File.AppendAllText(outputPath, text);
            }
        }

        private static long Timeit(Action action)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            action();
            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

    }
}
