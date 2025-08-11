using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using System.Text.Json;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace ZipExtractorFunction
{
    public class Function
    {
        private readonly IAmazonS3 _s3Client;
        private readonly IAmazonSQS _sqsClient;
        private readonly string _queueUrl;

        public Function()
        {
            _s3Client = new AmazonS3Client();
            _sqsClient = new AmazonSQSClient();
            _queueUrl = Environment.GetEnvironmentVariable("NOTIFICATION_QUEUE_URL");
        }

        /// <summary>
        /// Constructor for testing with dependency injection
        /// </summary>
        /// <param name="s3Client">S3 client for testing</param>
        /// <param name="sqsClient">SQS client for testing</param>
        /// <param name="queueUrl">SQS queue URL for testing</param>
        public Function(IAmazonS3 s3Client, IAmazonSQS sqsClient, string queueUrl)
        {
            _s3Client = s3Client;
            _sqsClient = sqsClient;
            _queueUrl = queueUrl;
        }

        /// <summary>
        /// Lambda function handler that gets triggered when a zip file is uploaded to the zip folder
        /// </summary>
        /// <param name="evnt">S3 event notification</param>
        /// <param name="context">Lambda context</param>
        /// <returns>Task</returns>
        public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            foreach (var record in evnt.Records)
            {
                var bucketName = record.S3.Bucket.Name;
                var objectKey = record.S3.Object.Key;

                context.Logger.LogInformation($"Processing zip file: s3://{bucketName}/{objectKey}");

                try
                {
                    // Validate that the file is in the zip folder and is a zip file
                    if (!objectKey.StartsWith("zip/", StringComparison.OrdinalIgnoreCase) ||
                        !objectKey.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
                    {
                        context.Logger.LogInformation($"Skipping file {objectKey} - not a zip file in zip folder");
                        continue;
                    }

                    // Download the zip file
                    context.Logger.LogInformation($"Downloading zip file from S3: {objectKey}");
                    var getRequest = new GetObjectRequest
                    {
                        BucketName = bucketName,
                        Key = objectKey
                    };

                    using var response = await _s3Client.GetObjectAsync(getRequest);
                    using var responseStream = response.ResponseStream;
                    using var memoryStream = new MemoryStream();
                    
                    await responseStream.CopyToAsync(memoryStream);
                    memoryStream.Position = 0;

                    // Extract the zip file
                    var extractionResult = await ExtractZipToS3(bucketName, objectKey, memoryStream, context);

                    // Send SQS notification
                    if (!string.IsNullOrEmpty(_queueUrl))
                    {
                        await SendUnzipNotification(bucketName, objectKey, extractionResult, context);
                    }
                    else
                    {
                        context.Logger.LogWarning("NOTIFICATION_QUEUE_URL not configured - skipping SQS notification");
                    }

                    context.Logger.LogInformation($"Successfully processed zip file: {objectKey}");
                }
                catch (Exception ex)
                {
                    context.Logger.LogError($"Error processing zip file {objectKey}: {ex.Message}");
                    
                    // Send failure notification
                    if (!string.IsNullOrEmpty(_queueUrl))
                    {
                        await SendUnzipNotification(bucketName, objectKey, new ExtractionResult 
                        { 
                            Success = false, 
                            ErrorMessage = ex.Message
                        }, context);
                    }
                    
                    throw; // Re-throw to mark Lambda execution as failed
                }
            }
        }

        /// <summary>
        /// Extracts a zip file and uploads each entry to the unzip folder in S3
        /// </summary>
        /// <param name="bucketName">S3 bucket name</param>
        /// <param name="zipObjectKey">Original zip file key</param>
        /// <param name="zipStream">Stream containing the zip file data</param>
        /// <param name="context">Lambda context for logging</param>
        /// <returns>Extraction result</returns>
        private async Task<ExtractionResult> ExtractZipToS3(string bucketName, string zipObjectKey, Stream zipStream, ILambdaContext context)
        {
            var result = new ExtractionResult
            {
                Success = true
            };

            try
            {
                // Create a folder name based on the zip file name (without extension)
                var zipFileName = Path.GetFileNameWithoutExtension(zipObjectKey.Replace("zip/", ""));
                var extractFolder = $"unzip/{zipFileName}/";

                context.Logger.LogInformation($"Extracting to folder: {extractFolder}");

                using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read);
                int extractedCount = 0;
                
                foreach (var entry in archive.Entries)
                {
                    // Skip directories
                    if (string.IsNullOrEmpty(entry.Name))
                    {
                        context.Logger.LogInformation($"Skipping directory: {entry.FullName}");
                        continue;
                    }

                    try
                    {
                        // Create the S3 key for the extracted file
                        // Use only the filename to avoid nested folder structures from within the zip
                        var extractedKey = extractFolder + entry.Name;
                        
                        context.Logger.LogInformation($"Extracting file: {entry.FullName} -> {extractedKey}");

                        // Open the entry stream
                        using var entryStream = entry.Open();
                        using var entryMemoryStream = new MemoryStream();
                        
                        await entryStream.CopyToAsync(entryMemoryStream);
                        entryMemoryStream.Position = 0;

                        // Determine content type based on file extension
                        var contentType = GetContentType(entry.Name);

                        // Upload the extracted file to S3
                        var putRequest = new PutObjectRequest
                        {
                            BucketName = bucketName,
                            Key = extractedKey,
                            InputStream = entryMemoryStream,
                            ContentType = contentType,
                            ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256
                        };

                        await _s3Client.PutObjectAsync(putRequest);
                        extractedCount++;
                        context.Logger.LogInformation($"Successfully uploaded: {extractedKey}");
                    }
                    catch (Exception ex)
                    {
                        context.Logger.LogError($"Error extracting file {entry.FullName}: {ex.Message}");
                        // Continue with other files even if one fails
                    }
                }

                // Set the count of successfully extracted files
                result.ExtractedFilesCount = extractedCount;
                result.ExtractedToFolder = extractFolder;
                context.Logger.LogInformation($"Extraction completed for {zipObjectKey}. Extracted {result.ExtractedFilesCount} files.");
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.ErrorMessage = ex.Message;
                context.Logger.LogError($"Error during extraction: {ex.Message}");
            }

            return result;
        }

        /// <summary>
        /// Sends a notification to SQS about the unzip completion
        /// </summary>
        /// <param name="bucketName">S3 bucket name</param>
        /// <param name="zipObjectKey">Original zip file key</param>
        /// <param name="extractionResult">Result of the extraction</param>
        /// <param name="context">Lambda context</param>
        /// <returns>Task</returns>
        private async Task SendUnzipNotification(string bucketName, string zipObjectKey, ExtractionResult extractionResult, ILambdaContext context)
        {
            try
            {
                var notification = new UnzipNotification
                {
                    SourceBucket = bucketName,
                    SourceZipFile = zipObjectKey,
                    Success = extractionResult.Success,
                    ExtractedToFolder = extractionResult.ExtractedToFolder,
                    ExtractedFilesCount = extractionResult.ExtractedFilesCount,
                    ErrorMessage = extractionResult.ErrorMessage,
                    ProcessedAt = DateTime.UtcNow,
                    RequestId = context.AwsRequestId
                };

                var messageBody = JsonSerializer.Serialize(notification, new JsonSerializerOptions 
                { 
                    WriteIndented = true 
                });

                var sendRequest = new SendMessageRequest
                {
                    QueueUrl = _queueUrl,
                    MessageBody = messageBody,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        ["SourceBucket"] = new MessageAttributeValue 
                        { 
                            StringValue = bucketName, 
                            DataType = "String" 
                        },
                        ["Success"] = new MessageAttributeValue 
                        { 
                            StringValue = extractionResult.Success.ToString(), 
                            DataType = "String" 
                        },
                        ["ExtractedFilesCount"] = new MessageAttributeValue 
                        { 
                            StringValue = extractionResult.ExtractedFilesCount.ToString(), 
                            DataType = "Number" 
                        }
                    }
                };

                await _sqsClient.SendMessageAsync(sendRequest);
                context.Logger.LogInformation($"Sent SQS notification for {zipObjectKey} to queue {_queueUrl}");
            }
            catch (Exception ex)
            {
                context.Logger.LogError($"Failed to send SQS notification: {ex.Message}");
                // Don't throw here - we don't want SQS failures to fail the extraction
            }
        }

        /// <summary>
        /// Determines the content type based on file extension
        /// </summary>
        /// <param name="fileName">File name</param>
        /// <returns>Content type string</returns>
        private static string GetContentType(string fileName)
        {
            var extension = Path.GetExtension(fileName).ToLowerInvariant();
            return extension switch
            {
                ".txt" => "text/plain",
                ".csv" => "text/csv",
                ".json" => "application/json",
                ".xml" => "application/xml",
                ".pdf" => "application/pdf",
                ".jpg" or ".jpeg" => "image/jpeg",
                ".png" => "image/png",
                ".gif" => "image/gif",
                ".tif" or ".tiff" => "image/tiff",
                ".zip" => "application/zip",
                ".html" or ".htm" => "text/html",
                ".css" => "text/css",
                ".js" => "application/javascript",
                _ => "application/octet-stream"
            };
        }
    }

    /// <summary>
    /// Result of zip extraction operation
    /// </summary>
    public class ExtractionResult
    {
        public bool Success { get; set; }
        public string ExtractedToFolder { get; set; } = string.Empty;
        public int ExtractedFilesCount { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
    }

    /// <summary>
    /// Notification message sent to SQS
    /// </summary>
    public class UnzipNotification
    {
        public string SourceBucket { get; set; } = string.Empty;
        public string SourceZipFile { get; set; } = string.Empty;
        public bool Success { get; set; }
        public string ExtractedToFolder { get; set; } = string.Empty;
        public int ExtractedFilesCount { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public DateTime ProcessedAt { get; set; }
        public string RequestId { get; set; } = string.Empty;
    }
}