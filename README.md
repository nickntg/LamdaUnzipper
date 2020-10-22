# LamdaUnzipper
This is a Lambda function in C# that accepts an S3 event and unzips the file it refers to in the same S3 path.

The code makes heavy use of ActionBlock that is in the [Dataflow TPL](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/dataflow-task-parallel-library) in order to achieve as much parallelism as possible.

Tested in eu-central-1 using a Lambda with the maximum memory of 3008 MB, the lambda unzipped a 1.4GB file in 169 seconds using a max degree of parallelism equals to 24 and consumed a maximum of 894MB. As with all things serverless, your mileage may vary depending on the size of the zip to be uncompressed and the size of individual files within the zip file.