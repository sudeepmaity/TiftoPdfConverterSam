using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text;
using System.Text.Json;
using System.IO;
using System;

using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SQS;
using Amazon.SQS.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GrouperFunction
{
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

    public class TiffGroupMessage
    {
        public string GroupKey { get; set; } = string.Empty;
        public string[] TiffFileKeys { get; set; } = Array.Empty<string>();
        public string SourceBucket { get; set; } = string.Empty;
        public string OutputBucket { get; set; } = string.Empty;
        public string OutputPrefix { get; set; } = string.Empty;
        public string ExtractedFolder { get; set; } = string.Empty;
    }

    public class Function
    {
        private readonly IAmazonS3 _s3Client;
        private readonly IAmazonSQS _sqsClient;

        public Function()
        {
            _s3Client = new AmazonS3Client();
            _sqsClient = new AmazonSQSClient();
        }

        public Function(IAmazonS3 s3Client, IAmazonSQS sqsClient)
        {
            _s3Client = s3Client;
            _sqsClient = sqsClient;
        }

        /// <summary>
        /// SQS event handler that processes unzip notifications and groups TIFF files
        /// </summary>
        public async Task<string> SqsFunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            var responses = new List<string>();

            foreach (var record in evnt.Records)
            {
                try
                {
                    context.Logger.LogInformation($"Processing SQS record: {record.MessageId}");
                    context.Logger.LogInformation($"SQS Message Body: {record.Body}");
                    context.Logger.LogInformation($"SQS Message Attributes: {JsonSerializer.Serialize(record.MessageAttributes)}");
                    
                    // Parse the unzip notification
                    var notification = JsonSerializer.Deserialize<UnzipNotification>(record.Body);
                    
                    if (notification == null || !notification.Success)
                    {
                        context.Logger.LogError($"Invalid or failed unzip notification: {record.Body}");
                        responses.Add($"Skipped invalid notification: {record.MessageId}");
                        continue;
                    }

                    // Process the extracted files and create groups
                    var groupingResult = await ProcessExtractedFiles(notification, context);
                    
                    if (groupingResult.Success)
                    {
                        responses.Add($"Successfully processed {groupingResult.GroupCount} groups from {record.MessageId}");
                    }
                    else
                    {
                        responses.Add($"Failed to process {record.MessageId}: {groupingResult.Error}");
                    }
                }
                catch (Exception ex)
                {
                    context.Logger.LogError($"Error processing SQS record {record.MessageId}: {ex.Message}");
                    responses.Add($"Error processing {record.MessageId}: {ex.Message}");
                }
            }

            return string.Join("; ", responses);
        }

        private async Task<(bool Success, int GroupCount, string Error)> ProcessExtractedFiles(
            UnzipNotification notification, ILambdaContext context)
        {
            try
            {
                context.Logger.LogInformation($"Processing extracted files from: {notification.ExtractedToFolder}");

                // Find and parse the index file
                var indexKey = $"{notification.ExtractedToFolder.TrimEnd('/')}/index";
                var parseResult = await ReadAndParseIndexFileFromS3(notification.SourceBucket, indexKey, context);

                if (!parseResult.Success)
                {
                    return (false, 0, parseResult.Error);
                }

                // Group the files by batch_number and item_number (same logic as original)
                var groupedFiles = ParseAndGroupFiles(parseResult.IndexContent, context);

                if (groupedFiles.Count == 0)
                {
                    return (false, 0, "No valid groups found in index file");
                }

                // Send one SQS message per group to the PDF creation queue
                var sqsResult = await SendGroupMessages(groupedFiles, notification, context);

                return (sqsResult.Success, groupedFiles.Count, sqsResult.Error);
            }
            catch (Exception ex)
            {
                return (false, 0, $"Error processing extracted files: {ex.Message}");
            }
        }

        private async Task<(bool Success, string IndexContent, string Error)> ReadAndParseIndexFileFromS3(
            string bucketName, string indexKey, ILambdaContext context)
        {
            try
            {
                context.Logger.LogInformation($"Reading index file from s3://{bucketName}/{indexKey}");

                var getRequest = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = indexKey
                };

                using var response = await _s3Client.GetObjectAsync(getRequest);
                using var reader = new StreamReader(response.ResponseStream);
                var content = await reader.ReadToEndAsync();

                context.Logger.LogInformation($"Successfully read index file, size: {content.Length} bytes");
                return (true, content, string.Empty);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return (false, string.Empty, $"Index file not found at s3://{bucketName}/{indexKey}");
            }
            catch (Exception ex)
            {
                return (false, string.Empty, $"Error reading index file: {ex.Message}");
            }
        }

        private Dictionary<string, string[]> ParseAndGroupFiles(string indexContent, ILambdaContext context)
        {
            try
            {
                var lines = indexContent.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                                      .Select(line => line.Trim())
                                      .Where(line => !string.IsNullOrEmpty(line))
                                      .ToList();

                context.Logger.LogInformation($"Parsing {lines.Count} lines from index file");

                // Parse all entries using the same CSV parsing logic
                var parsedEntries = lines.Select((line, index) =>
                {
                    var fields = ParseCsvLine(line);
                    
                    return new
                    {
                        lineNumber = index + 1,
                        rawLine = line,
                        fields = fields,
                        parsedData = fields.Length >= 4 ? new
                        {
                            batch_number = fields.Length > 0 ? fields[0].Trim() : string.Empty,
                            item_number = fields.Length > 1 ? fields[1].Trim() : string.Empty,
                            payment_type = fields.Length > 2 ? fields[2].Trim() : string.Empty,
                            file_name = fields.Length > 3 ? fields[3].Trim() : string.Empty
                        } : null
                    };
                }).ToArray();

                // Group by batch_number and item_number, combining both C and M files
                var groupedByBatchNumberAndItemNumber = parsedEntries
                    .Where(entry => entry.parsedData != null &&
                           !string.IsNullOrEmpty(entry.parsedData.batch_number) &&
                           !string.IsNullOrEmpty(entry.parsedData.item_number))
                    .GroupBy(entry => new { entry.parsedData.batch_number, entry.parsedData.item_number })
                    .ToDictionary(
                        group => $"{group.Key.batch_number}_{group.Key.item_number}",
                        group => group
                            .Where(entry => !string.IsNullOrWhiteSpace(entry.parsedData.file_name) &&
                                          !string.IsNullOrWhiteSpace(entry.parsedData.payment_type) &&
                                          (entry.parsedData.payment_type.Trim().Equals("M", StringComparison.OrdinalIgnoreCase) ||
                                           entry.parsedData.payment_type.Trim().Equals("C", StringComparison.OrdinalIgnoreCase)))
                            .Select(entry => entry.parsedData.file_name.Trim())
                            .ToArray()
                    );

                context.Logger.LogInformation($"Created {groupedByBatchNumberAndItemNumber.Count} groups");

                return groupedByBatchNumberAndItemNumber;
            }
            catch (Exception ex)
            {
                context.Logger.LogError($"Error parsing index content: {ex.Message}");
                return new Dictionary<string, string[]>();
            }
        }

        private static string[] ParseCsvLine(string csvLine)
        {
            var fields = new List<string>();
            var currentField = new StringBuilder();
            bool inQuotes = false;

            for (int i = 0; i < csvLine.Length; i++)
            {
                char c = csvLine[i];

                if (c == '"')
                {
                    if (inQuotes && i + 1 < csvLine.Length && csvLine[i + 1] == '"')
                    {
                        currentField.Append('"');
                        i++; // Skip next quote
                    }
                    else
                    {
                        inQuotes = !inQuotes;
                    }
                }
                else if (c == ',' && !inQuotes)
                {
                    fields.Add(currentField.ToString());
                    currentField.Clear();
                }
                else
                {
                    currentField.Append(c);
                }
            }

            fields.Add(currentField.ToString());
            return fields.ToArray();
        }

        private async Task<(bool Success, string Error)> SendGroupMessages(
            Dictionary<string, string[]> groupedFiles, UnzipNotification notification, ILambdaContext context)
        {
            try
            {
                var queueUrl = Environment.GetEnvironmentVariable("PDF_CREATION_QUEUE_URL");
                if (string.IsNullOrEmpty(queueUrl))
                {
                    return (false, "PDF_CREATION_QUEUE_URL environment variable not set");
                }

                var outputBucket = Environment.GetEnvironmentVariable("OUTPUT_BUCKET");
                var outputPrefix = Environment.GetEnvironmentVariable("OUTPUT_PREFIX") ?? string.Empty;

                context.Logger.LogInformation($"Sending {groupedFiles.Count} group messages to queue: {queueUrl}");

                var tasks = groupedFiles.Select(async kvp =>
                {
                    var groupMessage = new TiffGroupMessage
                    {
                        GroupKey = kvp.Key,
                        TiffFileKeys = kvp.Value,
                        SourceBucket = notification.SourceBucket,
                        OutputBucket = outputBucket,
                        OutputPrefix = outputPrefix,
                        ExtractedFolder = notification.ExtractedToFolder
                    };

                    var messageBody = JsonSerializer.Serialize(groupMessage);
                    
                    var sendRequest = new SendMessageRequest
                    {
                        QueueUrl = queueUrl,
                        MessageBody = messageBody,
                        MessageGroupId = $"group-{kvp.Key}", // For FIFO queues
                        MessageDeduplicationId = $"{notification.RequestId}-{kvp.Key}" // For FIFO queues
                    };

                    try
                    {
                        await _sqsClient.SendMessageAsync(sendRequest);
                        context.Logger.LogInformation($"Sent group message for: {kvp.Key}");
                        return true;
                    }
                    catch (Exception ex)
                    {
                        context.Logger.LogError($"Failed to send message for group {kvp.Key}: {ex.Message}");
                        return false;
                    }
                });

                var results = await Task.WhenAll(tasks);
                var successCount = results.Count(r => r);

                if (successCount == groupedFiles.Count)
                {
                    return (true, string.Empty);
                }
                else
                {
                    return (false, $"Only {successCount}/{groupedFiles.Count} messages sent successfully");
                }
            }
            catch (Exception ex)
            {
                return (false, $"Error sending group messages: {ex.Message}");
            }
        }
    }
}