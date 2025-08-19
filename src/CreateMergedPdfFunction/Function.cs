using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text.Json;
using System.IO;
using System;

using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.S3;
using Amazon.S3.Model;
using Aspose.Pdf;
using IOPath = System.IO.Path;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace CreateMergedPdfFunction
{
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

        public Function()
        {
            _s3Client = new AmazonS3Client();
        }

        public Function(IAmazonS3 s3Client)
        {
            _s3Client = s3Client;
        }

        /// <summary>
        /// Sets the Aspose.PDF license from a license file.
        /// </summary>
        /// <param name="licenseFilePath">Path to the license file. If null, looks for 'Aspose.Total.lic' in the application directory.</param>
        /// <returns>True if license was set successfully, false otherwise.</returns>
        public static bool SetAsposeLicense(string licenseFilePath = null)
        {
            try
            {
                // Default license file path if not provided
                if (string.IsNullOrEmpty(licenseFilePath))
                {
                    licenseFilePath = IOPath.Combine(AppDomain.CurrentDomain.BaseDirectory, "Aspose.Total.lic");
                }

                // Check if license file exists
                if (!File.Exists(licenseFilePath))
                {
                    // Try alternative common license file names
                    string[] alternativeNames = { "Aspose.PDF.lic", "license.lic", "Aspose.lic" };
                    string baseDirectory = IOPath.GetDirectoryName(licenseFilePath) ?? AppDomain.CurrentDomain.BaseDirectory;
                    
                    foreach (string altName in alternativeNames)
                    {
                        string altPath = IOPath.Combine(baseDirectory, altName);
                        if (File.Exists(altPath))
                        {
                            licenseFilePath = altPath;
                            break;
                        }
                    }
                    
                    // If still not found, return false
                    if (!File.Exists(licenseFilePath))
                    {
                        return false;
                    }
                }

                // Set the license
                var license = new Aspose.Pdf.License();
                license.SetLicense(licenseFilePath);
                
                return true;
            }
            catch (Exception)
            {
                // License setting failed
                return false;
            }
        }

        /// <summary>
        /// SQS event handler that processes TIFF group messages and creates merged PDFs
        /// </summary>
        public async Task<string> SqsFunctionHandler(SQSEvent evnt, ILambdaContext context)
        {
            // Try to set the Aspose license
            bool licenseSet = SetAsposeLicense();
            if (!licenseSet)
            {
                context.Logger.LogWarning("Aspose license not found or failed to set. Running in evaluation mode.");
            }

            var responses = new List<string>();

            foreach (var record in evnt.Records)
            {
                try
                {
                    context.Logger.LogInformation($"Processing SQS record: {record.MessageId}");
                    context.Logger.LogInformation($"SQS Message Body: {record.Body}");
                    context.Logger.LogInformation($"SQS Message Attributes: {JsonSerializer.Serialize(record.MessageAttributes)}");
                    
                    // Parse the TIFF group message
                    var groupMessage = JsonSerializer.Deserialize<TiffGroupMessage>(record.Body);
                    
                    if (groupMessage == null || string.IsNullOrEmpty(groupMessage.GroupKey))
                    {
                        context.Logger.LogError($"Invalid group message: {record.Body}");
                        responses.Add($"Skipped invalid message: {record.MessageId}");
                        continue;
                    }

                    // Create the merged PDF for this group
                    var pdfResult = await CreateMergedPdfForGroup(groupMessage, context);
                    
                    if (pdfResult.Success)
                    {
                        responses.Add($"Successfully created PDF for group {groupMessage.GroupKey}: {pdfResult.OutputKey}");
                    }
                    else
                    {
                        responses.Add($"Failed to create PDF for group {groupMessage.GroupKey}: {pdfResult.Error}");
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

        private async Task<(bool Success, string OutputKey, string Error)> CreateMergedPdfForGroup(
            TiffGroupMessage groupMessage, ILambdaContext context)
        {
            try
            {
                context.Logger.LogInformation($"Creating PDF for group {groupMessage.GroupKey} with {groupMessage.TiffFileKeys.Length} TIFF files");

                if (groupMessage.TiffFileKeys.Length == 0)
                {
                    return (false, string.Empty, "No TIFF files in group");
                }

                if (string.IsNullOrEmpty(groupMessage.OutputBucket))
                {
                    return (false, string.Empty, "Output bucket not specified");
                }

                // Create PDF document for this group
                using var pdfDocument = new Document();

                // Set basic document info
                pdfDocument.Info.Title = $"Merged PDF for {groupMessage.GroupKey}";
                pdfDocument.Info.Author = "CreateMergedPdfFunction";
                pdfDocument.Info.Subject = $"Group {groupMessage.GroupKey}";
                pdfDocument.Info.Creator = "Aspose.PDF";

                int processedFiles = 0;
                bool licenseSet = SetAsposeLicense();
                int maxFiles = licenseSet ? int.MaxValue : 4; // Limit for evaluation mode

                // Process TIFF files (limit to 4 for evaluation mode if no license)
                foreach (string tiffFilename in groupMessage.TiffFileKeys.Take(maxFiles))
                {
                    try
                    {
                        // Find the actual TIFF file in S3
                        string actualTiffKey = await FindTiffFileInS3(groupMessage.SourceBucket, 
                            groupMessage.ExtractedFolder, tiffFilename, context);

                        if (actualTiffKey != null)
                        {
                            context.Logger.LogInformation($"Found TIFF file in S3: s3://{groupMessage.SourceBucket}/{actualTiffKey}");

                            // Download the TIFF file from S3
                            var getRequest = new GetObjectRequest
                            {
                                BucketName = groupMessage.SourceBucket,
                                Key = actualTiffKey
                            };

                            using var response = await _s3Client.GetObjectAsync(getRequest);
                            using var memoryStream = new MemoryStream();
                            await response.ResponseStream.CopyToAsync(memoryStream);
                            memoryStream.Position = 0;

                            // Create a page
                            var page = pdfDocument.Pages.Add();

                            // Set page size explicitly
                            page.SetPageSize(PageSize.A4.Width, PageSize.A4.Height);

                            // Create and add image sized to the content area (A4 minus 36pt margins)
                            var image = new Aspose.Pdf.Image();
                            image.ImageStream = memoryStream;
                            image.FixWidth = 523d;  // A4 width (595) - 72pt margins
                            image.FixHeight = 770d; // A4 height (842) - 72pt margins

                            page.Paragraphs.Add(image);
                            processedFiles++;

                            context.Logger.LogInformation($"Added {IOPath.GetFileName(actualTiffKey)} to group {groupMessage.GroupKey}");
                        }
                        else
                        {
                            context.Logger.LogWarning($"TIFF file not found: {tiffFilename}");
                        }
                    }
                    catch (Exception ex)
                    {
                        context.Logger.LogWarning($"Failed to add {tiffFilename}: {ex.Message}");
                    }
                }

                if (processedFiles > 0)
                {
                    // Save to memory stream, then upload to S3
                    using var pdfStream = new MemoryStream();
                    var saveOptions = new Aspose.Pdf.PdfSaveOptions();
                    pdfDocument.Save(pdfStream, saveOptions);
                    pdfStream.Position = 0;

                    string normalizedPrefix = string.Join("/",
                        (groupMessage.OutputPrefix ?? string.Empty)
                            .Split(new[] { '/', '\\' }, StringSplitOptions.RemoveEmptyEntries));

                    string objectKey = string.IsNullOrEmpty(normalizedPrefix)
                        ? $"{groupMessage.GroupKey}_Merged.pdf"
                        : $"{normalizedPrefix}/{groupMessage.GroupKey}_Merged.pdf";

                    // Upload to S3
                    var putRequest = new PutObjectRequest
                    {
                        BucketName = groupMessage.OutputBucket,
                        Key = objectKey,
                        InputStream = pdfStream,
                        ContentType = "application/pdf",
                        ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256
                    };

                    await _s3Client.PutObjectAsync(putRequest);

                    context.Logger.LogInformation($"Uploaded PDF for group {groupMessage.GroupKey} to s3://{groupMessage.OutputBucket}/{objectKey}");
                    return (true, objectKey, string.Empty);
                }
                else
                {
                    return (false, string.Empty, "No files were successfully processed");
                }
            }
            catch (Exception ex)
            {
                return (false, string.Empty, $"Error creating PDF: {ex.Message}");
            }
        }

        private async Task<string> FindTiffFileInS3(string bucketName, string folderPath, string filename, ILambdaContext context = null)
        {
            try
            {
                // Ensure folder path ends with /
                if (!folderPath.EndsWith("/"))
                {
                    folderPath += "/";
                }

                // Get filename without extension to try different variations
                string nameWithoutExt = IOPath.GetFileNameWithoutExtension(filename);
                string[] tiffExtensions = { "", ".tiff", ".tif", ".TIFF", ".TIF" };

                // Try each TIFF extension
                foreach (string extension in tiffExtensions)
                {
                    string testKey = folderPath + nameWithoutExt + extension;
                    
                    try
                    {
                        // Check if the object exists
                        var headRequest = new GetObjectMetadataRequest
                        {
                            BucketName = bucketName,
                            Key = testKey
                        };

                        await _s3Client.GetObjectMetadataAsync(headRequest);
                        return testKey; // File exists, return the key
                    }
                    catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        // File not found with this extension, try next
                        continue;
                    }
                }

                // If no direct match found, try case-insensitive search
                context?.Logger.LogInformation($"Direct file not found, trying case-insensitive search for: {filename}");

                var listRequest = new ListObjectsV2Request
                {
                    BucketName = bucketName,
                    Prefix = folderPath,
                    MaxKeys = 1000
                };

                var response = await _s3Client.ListObjectsV2Async(listRequest);
                
                // Look for a case-insensitive match
                var matchingKey = response.S3Objects
                    .Select(obj => obj.Key)
                    .FirstOrDefault(key =>
                    {
                        var keyFileName = IOPath.GetFileName(key);
                        var keyNameWithoutExt = IOPath.GetFileNameWithoutExtension(keyFileName);
                        return string.Equals(keyNameWithoutExt, nameWithoutExt, StringComparison.OrdinalIgnoreCase);
                    });

                if (matchingKey != null)
                {
                    context?.Logger.LogInformation($"Found case-insensitive match: {matchingKey}");
                    return matchingKey;
                }

                return null; // File not found
            }
            catch (Exception ex)
            {
                context?.Logger.LogError($"Error searching for TIFF file {filename}: {ex.Message}");
                return null;
            }
        }
    }
}