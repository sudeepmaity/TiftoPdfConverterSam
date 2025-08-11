using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.Lambda.TestUtilities;
using Amazon.S3;
using Amazon.S3.Model;
using Moq;
using Xunit;
using ZipExtractorFunction;

namespace ZipExtractorFunction.Tests
{
    public class FunctionTest
    {
        [Fact]
        public async Task TestZipExtractionHandler()
        {
            // Arrange
            var mockS3Client = new Mock<IAmazonS3>();
            var function = new Function(mockS3Client.Object);
            var context = new TestLambdaContext();

            // Create a test zip file in memory
            var zipStream = CreateTestZipFile();
            
            // Mock S3 GetObject response
            var getObjectResponse = new GetObjectResponse
            {
                ResponseStream = zipStream
            };
            
            mockS3Client.Setup(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()))
                       .ReturnsAsync(getObjectResponse);

            // Mock S3 PutObject response
            mockS3Client.Setup(x => x.PutObjectAsync(It.IsAny<PutObjectRequest>(), It.IsAny<CancellationToken>()))
                       .ReturnsAsync(new PutObjectResponse());

            // Create S3 event
            var s3Event = new S3Event
            {
                Records = new List<S3Event.S3EventNotificationRecord>
                {
                    new S3Event.S3EventNotificationRecord
                    {
                        S3 = new S3Event.S3Entity
                        {
                            Bucket = new S3Event.S3BucketEntity { Name = "test-bucket" },
                            Object = new S3Event.S3ObjectEntity { Key = "zip/test-file.zip" }
                        }
                    }
                }
            };

            // Act
            await function.FunctionHandler(s3Event, context);

            // Assert
            mockS3Client.Verify(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()), Times.Once);
            mockS3Client.Verify(x => x.PutObjectAsync(It.Is<PutObjectRequest>(req => 
                req.Key.StartsWith("unzip/test-file/")), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        }

        [Fact]
        public async Task TestSkipNonZipFiles()
        {
            // Arrange
            var mockS3Client = new Mock<IAmazonS3>();
            var function = new Function(mockS3Client.Object);
            var context = new TestLambdaContext();

            // Create S3 event with non-zip file
            var s3Event = new S3Event
            {
                Records = new List<S3Event.S3EventNotificationRecord>
                {
                    new S3Event.S3EventNotificationRecord
                    {
                        S3 = new S3Event.S3Entity
                        {
                            Bucket = new S3Event.S3BucketEntity { Name = "test-bucket" },
                            Object = new S3Event.S3ObjectEntity { Key = "zip/test-file.txt" }
                        }
                    }
                }
            };

            // Act
            await function.FunctionHandler(s3Event, context);

            // Assert - Should not call S3 operations for non-zip files
            mockS3Client.Verify(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact]
        public async Task TestSkipFilesNotInZipFolder()
        {
            // Arrange
            var mockS3Client = new Mock<IAmazonS3>();
            var function = new Function(mockS3Client.Object);
            var context = new TestLambdaContext();

            // Create S3 event with zip file not in zip folder
            var s3Event = new S3Event
            {
                Records = new List<S3Event.S3EventNotificationRecord>
                {
                    new S3Event.S3EventNotificationRecord
                    {
                        S3 = new S3Event.S3Entity
                        {
                            Bucket = new S3Event.S3BucketEntity { Name = "test-bucket" },
                            Object = new S3Event.S3ObjectEntity { Key = "other-folder/test-file.zip" }
                        }
                    }
                }
            };

            // Act
            await function.FunctionHandler(s3Event, context);

            // Assert - Should not call S3 operations for files not in zip folder
            mockS3Client.Verify(x => x.GetObjectAsync(It.IsAny<GetObjectRequest>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        private static MemoryStream CreateTestZipFile()
        {
            var zipStream = new MemoryStream();
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, true))
            {
                // Add a test text file
                var entry1 = archive.CreateEntry("test1.txt");
                using (var entryStream = entry1.Open())
                {
                    using (var writer = new StreamWriter(entryStream))
                    {
                        writer.Write("This is test content 1");
                    }
                }

                // Add another test file in a subdirectory
                var entry2 = archive.CreateEntry("subfolder/test2.txt");
                using (var entryStream = entry2.Open())
                {
                    using (var writer = new StreamWriter(entryStream))
                    {
                        writer.Write("This is test content 2");
                    }
                }
            }

            zipStream.Position = 0;
            return zipStream;
        }
    }
}