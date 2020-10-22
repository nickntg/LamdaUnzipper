using System;
using Amazon.S3.Model;

namespace LambdaUnzipper
{
    public class FileProcessor
    {
        public void ProcessStream(FileInfo fi)
        {
            try
            {
                ProcessInternal(fi);
            }
            catch (Exception e)
            {
                fi.Context.Logger.LogLine($"Error unzipping {fi.FileName}\r\n{e}");
            }
        }

        private void ProcessInternal(FileInfo fi)
        {
            var newName = $"{fi.BaseKey}{fi.FileName}";

            fi.Context.Logger.LogLine($"Uploading {fi.FileName} under {fi.BaseKey}, {fi.Data.Length} bytes");
            fi.Data.Position = 0;

            fi.S3Client.PutObjectAsync(new PutObjectRequest
            {
                Key = newName,
                BucketName = fi.Bucket,
                InputStream = fi.Data
            }).Wait();

            fi.Context.Logger.LogLine($"Upload of {fi.FileName} under {fi.BaseKey} complete");

            fi.Data.Dispose();
        }
    }
}
