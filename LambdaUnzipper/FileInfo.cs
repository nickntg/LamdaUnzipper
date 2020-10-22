using System.IO;
using Amazon.Lambda.Core;
using Amazon.S3;

namespace LambdaUnzipper
{
    public class FileInfo
    {
        public string FileName { get; set; }
        public MemoryStream Data { get; set; }
        public string Bucket { get; set; }
        public string BaseKey { get; set; }
        public IAmazonS3 S3Client { get; set; }
        public ILambdaContext Context { get; set; }
    }
}
