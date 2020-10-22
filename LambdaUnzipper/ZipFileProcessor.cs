using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using Amazon.Lambda.Core;
using Amazon.S3.Model;
using ICSharpCode.SharpZipLib.Core;
using ICSharpCode.SharpZipLib.Zip;

namespace LambdaUnzipper
{
    public class ZipFileProcessor
    {
        private const int DegreeOfParallelism         = 24;
        private const int MaxMessagesWaitingThreshold = 36;
        private const int LowMessagesWaitingThreshold = 12;

        public void ProcessFile(ZipFileInfo zipFileInfo)
        {
            try
            {
                ProcessInternal(zipFileInfo);
            }
            catch (Exception e)
            {
                zipFileInfo.Context.Logger.LogLine($"Error while processing {zipFileInfo.Key}\r\n{e}");
            }
        }

        private void ProcessInternal(ZipFileInfo zipFileInfo)
        {
            var block = new ActionBlock<FileInfo>(fi => new FileProcessor().ProcessStream(fi), new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = false,
                MaxDegreeOfParallelism = DegreeOfParallelism,
                SingleProducerConstrained = true
            });

            zipFileInfo.Context.Logger.LogLine($"Retrieving {zipFileInfo.Key} from S3");

            using (var stream = zipFileInfo.S3Client
                .GetObjectAsync(new GetObjectRequest
                {
                    Key = zipFileInfo.Key,
                    BucketName = zipFileInfo.Bucket
                }).Result
                .ResponseStream)
            {
                zipFileInfo.Context.Logger.LogLine($"Reading {zipFileInfo.Key} from S3");
                
                var baseKey = GetBaseS3Key(zipFileInfo.Key);
                using (var zipInputStream = new ZipInputStream(stream))
                {
                    var zipEntry = zipInputStream.GetNextEntry();

                    while (zipEntry != null)
                    {
                        Breather(block, zipFileInfo.Context.Logger);

                        EnqueueZipEntry(zipFileInfo, zipEntry, zipInputStream, baseKey, block);

                        zipEntry = zipInputStream.GetNextEntry();
                    }
                }
            }

            zipFileInfo.Context.Logger.LogLine($"Waiting {zipFileInfo.Key} extraction to complete");

            block.Complete();
            block.Completion.Wait();

            zipFileInfo.Context.Logger.LogLine($"{zipFileInfo.Key} extraction completed");
        }

        private void Breather(ActionBlock<FileInfo> block, ILambdaLogger logger)
        {
            // The idea here is to look out for a condition where we have too many
            // messages waiting to be processed. If that happens, we want to wait
            // a bit until some messages are completed. If we do not, we will keep
            // reading the zip file and queuing messages for processing - this would
            // normally not be bad but would put memory pressure on the Lambda and we 
            // might not be able to sustain that and get an out of memory exception if
            // the zip file is too big.

            if (block.InputCount > MaxMessagesWaitingThreshold)
            {
                logger.LogLine("Waiting for messages to be consumed");
                while (block.InputCount > LowMessagesWaitingThreshold)
                {
                    // Task.Delay() seems to throw an exception in Lambda.
                    Thread.Sleep(10);
                }
                logger.LogLine("Resuming");
            }
        }

        private void EnqueueZipEntry(ZipFileInfo zipFileInfo,
            ZipEntry zipEntry, 
            ZipInputStream zipInputStream, 
            string baseKey,
            ActionBlock<FileInfo> block)
        {
            var entryFileName = zipEntry.Name;

            if (!entryFileName.EndsWith("/"))
            {
                zipFileInfo.Context.Logger.LogLine($"Reading {zipEntry.Name} for processing from zip {zipFileInfo.Key}");

                var buffer = new byte[4096];
                var data = new MemoryStream();
                StreamUtils.Copy(zipInputStream, data, buffer);

                zipFileInfo.Context.Logger.LogLine($"Queueing {zipEntry.Name} for processing from zip {zipFileInfo.Key}");

                block.Post(new FileInfo
                {
                    Data = data,
                    FileName = zipEntry.Name,
                    Bucket = zipFileInfo.Bucket,
                    BaseKey = baseKey,
                    Context = zipFileInfo.Context,
                    S3Client = zipFileInfo.S3Client
                });
            }
        }

        private string GetBaseS3Key(string path)
        {
            var index = path.LastIndexOf("/", StringComparison.Ordinal);
            return index == 0 ? "" : path.Substring(0, index+1);
        }
    }
}