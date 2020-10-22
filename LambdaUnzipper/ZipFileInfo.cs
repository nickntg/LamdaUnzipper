using Amazon.Lambda.Core;
using Amazon.S3;

namespace LambdaUnzipper
{
    public class ZipFileInfo
    {
        public IAmazonS3 S3Client { get; set; }
        public ILambdaContext Context { get; set; }
        public string Bucket { get; set; }
        public string Key { get; set; }
    }
}