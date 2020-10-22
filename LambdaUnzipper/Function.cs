using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace LambdaUnzipper
{
    public class Function
    {
        private IAmazonS3 S3Client { get; set; }

        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        public async Task<string> FunctionHandler(S3Event events, ILambdaContext context)
        {
            if (events?.Records == null)
            {
                return null;
            }

            var block = new ActionBlock<ZipFileInfo>(info => new ZipFileProcessor().ProcessFile(info),
                new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = false,
                    MaxDegreeOfParallelism = 4
                });

            foreach (var ev in events.Records.Where(ev => ev != null))
            {
                context.Logger.LogLine($"Processing {ev.S3.Object.Key}");

                block.Post(new ZipFileInfo
                {
                    Bucket = ev.S3.Bucket.Name,
                    Context = context,
                    Key = ev.S3.Object.Key,
                    S3Client = S3Client
                });
            }

            context.Logger.LogLine("Waiting for zip file processors to complete");

            block.Complete();
            block.Completion.Wait();

            context.Logger.LogLine("Zip file processors completed");

            return null;
        }
    }
}