using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.IO;
using System;

using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using Aspose.Pdf;
using Aspose.Pdf.Devices;
using Aspose.Pdf.Text;

// REMOVED: using System.Drawing; - Not compatible with AWS Lambda

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace TiffToPdfConverter
{
    public class RequestBody
    {
        public string path { get; set; }
    }

    public class Function
    {
        private const string INDEX_FILE = "INDEX_FILE"; // Directory containing the index file
        private static readonly HttpClient client = new HttpClient();

        private static async Task<string> GetCallingIP()
        {
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Add("User-Agent", "AWS Lambda .Net Client");

            var msg = await client.GetStringAsync("http://checkip.amazonaws.com/").ConfigureAwait(continueOnCapturedContext:false);

            return msg.Replace("\n","");
        }

        public static string[] ParseCsvLine(string csvLine)
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
                        // Handle escaped quotes ("")
                        currentField.Append('"');
                        i++; // Skip next quote
                    }
                    else
                    {
                        // Toggle quote state
                        inQuotes = !inQuotes;
                    }
                }
                else if (c == ',' && !inQuotes)
                {
                    // End of field
                    fields.Add(currentField.ToString());
                    currentField.Clear();
                }
                else
                {
                    currentField.Append(c);
                }
            }
            
            // Add the last field
            fields.Add(currentField.ToString());
            
            return fields.ToArray();
        }

        private static async Task<(bool success, object jsonData, string error)> ReadAndParseIndexFile(ILambdaContext context = null)
        {
            try
            {
                // Look for index file inside the INDEX_FILE directory
                string indexDirectoryPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, INDEX_FILE);
                
                if (!Directory.Exists(indexDirectoryPath))
                {
                    return (false, null, $"Index directory not found at: {indexDirectoryPath}");
                }

                // Look for common index file names
                string[] possibleIndexFiles = { "index", "index.txt", "index.json", "index.csv" };
                string indexFilePath = null;

                foreach (string fileName in possibleIndexFiles)
                {
                    string testPath = Path.Combine(indexDirectoryPath, fileName);
                    if (File.Exists(testPath))
                    {
                        indexFilePath = testPath;
                        break;
                    }
                }

                if (indexFilePath == null)
                {
                    var availableFiles = Directory.GetFiles(indexDirectoryPath);
                    return (false, null, $"No index file found in directory: {indexDirectoryPath}. Available files: {string.Join(", ", availableFiles.Select(Path.GetFileName))}");
                }

                var content = await File.ReadAllTextAsync(indexFilePath);
                
                // Parse the content - assuming it's CSV-like data
                var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries)
                                  .Select(line => line.Trim())
                                  .Where(line => !string.IsNullOrEmpty(line))
                                  .ToList();

                // Parse all entries first
                var parsedEntries = lines.Select((line, index) => 
                {
                    var fields = ParseCsvLine(line);
                    
                    return new 
                    {
                        lineNumber = index + 1,
                        rawContent = line,
                        fields = fields,
                        fieldCount = fields.Length,
                        parsedData = fields.Length >= 5 ? new
                        {
                            date1 = fields.Length > 0 ? fields[0]?.Trim() : null,
                            date2 = fields.Length > 1 ? fields[1]?.Trim() : null,
                            id1 = fields.Length > 2 ? fields[2]?.Trim() : null,
                            lockbox = fields.Length > 3 ? fields[3]?.Trim() : null,
                            batch_number = fields.Length > 4 ? fields[4]?.Trim() : null,
                            item_number = fields.Length > 5 ? fields[5]?.Trim() : null,
                            page_number = fields.Length > 6 ? fields[6]?.Trim() : null,
                            payment_type = fields.Length > 7 ? fields[7]?.Trim() : null,
                            file_name = fields.Length > 8 ? fields[8]?.Trim() : null,
                            amount = fields.Length > 9 ? fields[9]?.Trim() : null,
                            account_number_1 = fields.Length > 10 ? fields[10]?.Trim() : null,
                            account_number_2 = fields.Length > 11 ? fields[11]?.Trim() : null,
                            account_number_3 = fields.Length > 12 ? fields[12]?.Trim() : null,
                            sequence_1 = fields.Length > 13 ? fields[13]?.Trim() : null,
                            sequence_2 = fields.Length > 14 ? fields[14]?.Trim() : null,
                            sequence_3 = fields.Length > 15 ? fields[15]?.Trim() : null,
                            entity_name = fields.Length > 17 ? fields[17]?.Trim()?.Replace("\"", "") : null,
                            sequence_4 = fields.Length > 19 ? fields[19]?.Trim()?.Replace("\"", "") : null
                        } : null
                    };
                }).ToArray();

                // Group by batch_number and item_number and collect M filenames
                var groupedByBatchNumberAnditem_number = parsedEntries
                    .Where(entry => entry.parsedData != null && 
                           !string.IsNullOrEmpty(entry.parsedData.batch_number) &&
                           !string.IsNullOrEmpty(entry.parsedData.item_number))
                    .GroupBy(entry => new { entry.parsedData.batch_number, entry.parsedData.item_number })
                    .ToDictionary(
                        group => $"{group.Key.batch_number}_{group.Key.item_number}",
                        group => new
                        {
                            batch_number = group.Key.batch_number,
                            item_number = group.Key.item_number,
                            totalFiles = group.Count(),
                            mFilenames = group
                                .Where(entry => !string.IsNullOrWhiteSpace(entry.parsedData.payment_type) &&
                                              entry.parsedData.payment_type.Trim().Equals("M", StringComparison.OrdinalIgnoreCase) && 
                                              !string.IsNullOrWhiteSpace(entry.parsedData.file_name) &&
                                              entry.parsedData.file_name.Trim().StartsWith("M", StringComparison.OrdinalIgnoreCase))
                                .Select(entry => entry.parsedData.file_name.Trim())
                                .ToArray(),
                            cFilenames = group
                                .Where(entry => !string.IsNullOrWhiteSpace(entry.parsedData.payment_type) &&
                                              entry.parsedData.payment_type.Trim().Equals("C", StringComparison.OrdinalIgnoreCase) && 
                                              !string.IsNullOrWhiteSpace(entry.parsedData.file_name) &&
                                              entry.parsedData.file_name.Trim().StartsWith("C", StringComparison.OrdinalIgnoreCase))
                                .Select(entry => entry.parsedData.file_name.Trim())
                                .ToArray(),
                            allEntries = group.ToArray()
                        }
                    );

                // Create summary of ALL filenames (C and M) by batch_number and item_number for PDF creation
                var allFilenamesByBatchNumberAnditem_number = groupedByBatchNumberAnditem_number
                    .Where(kvp => !string.IsNullOrEmpty(kvp.Key))
                    .ToDictionary(
                        kvp => kvp.Key,
                        kvp => kvp.Value.cFilenames.Concat(kvp.Value.mFilenames).Where(f => !string.IsNullOrEmpty(f)).ToArray()
                    );

                // Keep M filenames summary for backward compatibility in response
                var mFilenamesByBatchNumberAnditem_number = groupedByBatchNumberAnditem_number
                    .Where(kvp => !string.IsNullOrEmpty(kvp.Key))
                    .ToDictionary(
                        kvp => kvp.Key,
                        kvp => kvp.Value.mFilenames
                    );

                // Debug: Log the allFilenamesByBatchNumberAnditem_number with actual values
                context?.Logger?.LogInformation($"allFilenamesByBatchNumberAnditem_number: {JsonSerializer.Serialize(allFilenamesByBatchNumberAnditem_number, new JsonSerializerOptions { WriteIndented = true })}");
    
                // Create merged PDFs from ALL TIFF files (C and M together)
                var pdfCreationResult = CreateMergedPdfs(allFilenamesByBatchNumberAnditem_number, context);

                // Convert to JSON structure with CSV parsing and grouping
                var jsonData = new
                {
                    totalLines = lines.Count,
                    allFilenamesByBatchNumberAnditem_number = allFilenamesByBatchNumberAnditem_number,
                    mFilenamesByBatchNumberAnditem_number = mFilenamesByBatchNumberAnditem_number,
                    groupedByBatchNumberAnditem_number = groupedByBatchNumberAnditem_number,
                    entries = parsedEntries,
                    pdfCreation = new
                    {
                        success = pdfCreationResult.success,
                        createdPdfs = pdfCreationResult.createdPdfs,
                        error = pdfCreationResult.error,
                        note = "PDFs created from both C and M files combined"
                    },
                    metadata = new
                    {
                        filePath = indexFilePath,
                        fileName = Path.GetFileName(indexFilePath),
                        directory = indexDirectoryPath,
                        fileSize = content.Length,
                        parsedAt = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss UTC")
                    }
                };

                return (true, jsonData, null);
            }
            catch (IOException ex)
            {
                return (false, null, $"File I/O error: {ex.Message}");
            }
            catch (UnauthorizedAccessException ex)
            {
                return (false, null, $"Access denied: {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, null, $"Error parsing index file: {ex.Message}");
            }
        }

        private static (bool success, Dictionary<string, string> createdPdfs, string error) CreateMergedPdfs(Dictionary<string, string[]> allFilenamesByGroup, ILambdaContext context = null)
        {
            try
            {
                context?.Logger?.LogInformation("Starting PDF creation process");
                
                var createdPdfs = new Dictionary<string, string>();
                string tifFilesPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "M_TIF_Files");
                
                if (!Directory.Exists(tifFilesPath))
                {
                    return (false, null, $"TIFF files directory not found at: {tifFilesPath}");
                }

                // Debug: Check if dictionary is null or empty
                if (allFilenamesByGroup == null)
                {
                    return (false, null, "allFilenamesByGroup is null");
                }
                
                if (allFilenamesByGroup.Count == 0)
                {
                    return (false, null, "allFilenamesByGroup is empty");
                }

                // Process each group
                foreach (var kvp in allFilenamesByGroup)
                {
                    string groupKey = kvp.Key;
                    string[] tiffFilenames = kvp.Value;
                    
                    // Skip null or empty keys
                    if (string.IsNullOrWhiteSpace(groupKey))
                    {
                        context?.Logger?.LogInformation("Warning: Skipping group with null or empty key");
                        continue;
                    }
                    
                    if (tiffFilenames == null || tiffFilenames.Length == 0)
                    {
                        context?.Logger?.LogInformation($"Warning: No files for group {groupKey}");
                        continue;
                    }

                    context?.Logger?.LogInformation($"Processing group {groupKey} with {tiffFilenames.Length} files");
                    
                    Document pdfDocument = null;
                    try 
                    {
                        // Create PDF document for this group
                        pdfDocument = new Document();
                        int processedFiles = 0;
                        
                        // Process each TIFF file in the group (both C and M files)
                        foreach (string tiffFilename in tiffFilenames)
                        {
                            // Try to find the TIFF file with different extensions
                            string actualTiffPath = FindTiffFile(tifFilesPath, tiffFilename);
                            
                            if (actualTiffPath != null && File.Exists(actualTiffPath))
                            {
                                try
                                {
                                    // Create a new page for each TIFF file
                                    var page = pdfDocument.Pages.Add();
                                    
                                    // Add TIFF image directly using Aspose.PDF
                                    var image = new Aspose.Pdf.Image();
                                    image.File = actualTiffPath;
                                    
                                    // Set image to fit the page with margins
                                    image.FixWidth = page.PageInfo.Width - 72; // 36pt margin on each side
                                    image.FixHeight = page.PageInfo.Height - 72; // 36pt margin top/bottom
                                    
                                    page.Paragraphs.Add(image);
                                    processedFiles++;
                                    
                                    context?.Logger?.LogInformation($"Added {Path.GetFileName(actualTiffPath)} to group {groupKey}");
                                }
                                catch (Exception ex)
                                {
                                    // Log error but continue processing other files
                                    context?.Logger?.LogInformation($"Warning: Failed to process {Path.GetFileName(actualTiffPath)}: {ex.Message}");
                                }
                            }
                            else
                            {
                                context?.Logger?.LogInformation($"Warning: TIFF file not found: {tiffFilename}");
                            }
                        }
                        
                        context?.Logger?.LogInformation($"Processed {processedFiles}/{tiffFilenames.Length} files for group {groupKey}");
                        
                        // If no pages were added, add a placeholder page
                        if (pdfDocument.Pages.Count == 0)
                        {
                            var emptyPage = pdfDocument.Pages.Add();
                            var textFragment = new Aspose.Pdf.Text.TextFragment($"No valid TIFF files found for group {groupKey}");
                            emptyPage.Paragraphs.Add(textFragment);
                            context?.Logger?.LogInformation($"Added placeholder page for group {groupKey}");
                        }
                        
                        // Save the merged PDF to temp directory
                        string outputFileName = $"{groupKey}_Merged.pdf";
                        string outputDir = Path.Combine(Path.GetTempPath(), "OUTPUT");
                        
                        // Ensure OUTPUT directory exists
                        Directory.CreateDirectory(outputDir);
                        
                        string outputPath = Path.Combine(outputDir, outputFileName);
                        
                        // Save the PDF
                        pdfDocument.Save(outputPath);
                        
                        // Get file info
                        var fileInfo = new FileInfo(outputPath);
                        createdPdfs[groupKey] = $"Path: {outputPath}, Size: {fileInfo.Length} bytes, Pages: {pdfDocument.Pages.Count}";
                        
                        context?.Logger?.LogInformation($"Successfully created PDF for group {groupKey} at {outputPath}");
                    }
                    catch (Exception ex)
                    {
                        context?.Logger?.LogInformation($"Error processing group {groupKey}: {ex.Message}");
                        // Continue with next group
                    }
                    finally
                    {
                        // Clean up
                        pdfDocument?.Dispose();
                    }
                }
                
                if (createdPdfs.Count == 0)
                {
                    return (false, null, "No PDFs were successfully created");
                }
                
                return (true, createdPdfs, null);
            }
            catch (Exception ex)
            {
                return (false, null, $"Error creating merged PDFs: {ex.Message}");
            }
        }

        private static string FindTiffFile(string basePath, string filename)
        {
            // First try the exact filename as provided
            string fullPath = Path.Combine(basePath, filename);
            if (File.Exists(fullPath))
                return fullPath;

            // Get filename without extension to try different variations
            string nameWithoutExt = Path.GetFileNameWithoutExtension(filename);
            string[] tiffExtensions = { ".tiff", ".tif", ".TIFF", ".TIF" };

            // Try each TIFF extension
            foreach (string extension in tiffExtensions)
            {
                string testPath = Path.Combine(basePath, nameWithoutExt + extension);
                if (File.Exists(testPath))
                    return testPath;
            }

            // If original filename had no extension, try adding TIFF extensions
            if (string.IsNullOrEmpty(Path.GetExtension(filename)))
            {
                foreach (string extension in tiffExtensions)
                {
                    string testPath = Path.Combine(basePath, filename + extension);
                    if (File.Exists(testPath))
                        return testPath;
                }
            }

            // Return null if no file found
            return null;
        }

        private static bool IsTiffFile(string filePath)
        {
            if (string.IsNullOrEmpty(filePath))
                return false;

            string extension = Path.GetExtension(filePath).ToLowerInvariant();
            return extension == ".tiff" || extension == ".tif";
        }

        private static async Task<(bool success, string content, string error, long size)> DownloadIndexFile(string path)
        {
            try
            {
                // Check if this is a local file path or a web URL
                if (Uri.TryCreate(path, UriKind.Absolute, out Uri uri) && (uri.Scheme == "http" || uri.Scheme == "https"))
                {
                    // Handle web URL
                    string downloadUrl = path;
                    if (!path.Contains("index") && !Path.HasExtension(path))
                    {
                        downloadUrl = path.TrimEnd('/') + "/index";
                    }

                    client.DefaultRequestHeaders.Clear();
                    client.DefaultRequestHeaders.Add("User-Agent", "TiffToPdfConverter Lambda");

                    var response = await client.GetAsync(downloadUrl);
                    
                    if (response.IsSuccessStatusCode)
                    {
                        var content = await response.Content.ReadAsStringAsync();
                        var size = content.Length;
                        return (true, content, null, size);
                    }
                    else
                    {
                        return (false, null, $"HTTP {(int)response.StatusCode}: {response.ReasonPhrase}", 0);
                    }
                }
                else
                {
                    // Handle local file path
                    string filePath = path;
                    if (!path.Contains("index") && !Path.HasExtension(path))
                    {
                        filePath = Path.Combine(path, "index");
                    }

                    if (File.Exists(filePath))
                    {
                        var content = await File.ReadAllTextAsync(filePath);
                        var size = content.Length;
                        return (true, content, null, size);
                    }
                    else if (Directory.Exists(path) && !Path.HasExtension(path) && !path.Contains("index"))
                    {
                        // If it's a directory and no specific file was requested, list directory contents
                        try
                        {
                            var files = Directory.GetFiles(path);
                            var directories = Directory.GetDirectories(path);
                            
                            var directoryListing = "Directory Contents:\n\nFiles:\n";
                            directoryListing += files.Length > 0 ? string.Join("\n", files.Select(f => Path.GetFileName(f))) : "No files found";
                            
                            directoryListing += "\n\nDirectories:\n";
                            directoryListing += directories.Length > 0 ? string.Join("\n", directories.Select(d => Path.GetFileName(d))) : "No directories found";
                            
                            return (true, directoryListing, null, directoryListing.Length);
                        }
                        catch (UnauthorizedAccessException)
                        {
                            return (false, null, $"Access denied to directory: {path}", 0);
                        }
                    }
                    else
                    {
                        return (false, null, $"File not found: {filePath}", 0);
                    }
                }
            }
            catch (HttpRequestException ex)
            {
                return (false, null, $"Network error: {ex.Message}", 0);
            }
            catch (TaskCanceledException ex)
            {
                return (false, null, $"Request timeout: {ex.Message}", 0);
            }
            catch (IOException ex)
            {
                return (false, null, $"File I/O error: {ex.Message}", 0);
            }
            catch (UnauthorizedAccessException ex)
            {
                return (false, null, $"Access denied: {ex.Message}", 0);
            }
            catch (Exception ex)
            {
                return (false, null, $"Download error: {ex.Message}", 0);
            }
        }

        public async Task<APIGatewayProxyResponse> FunctionHandler(APIGatewayProxyRequest apigProxyEvent, ILambdaContext context)
        {
            try
            {
                // Parse the request body
                RequestBody requestBody = null;
                if (!string.IsNullOrEmpty(apigProxyEvent.Body))
                {
                    requestBody = JsonSerializer.Deserialize<RequestBody>(apigProxyEvent.Body);
                }

                if (requestBody == null || string.IsNullOrEmpty(requestBody.path))
                {
                    var location = await GetCallingIP();
                    return new APIGatewayProxyResponse
                    {
                        Body = JsonSerializer.Serialize(new { 
                            pdfCreation = new { success = false, error = "Request body must contain a 'path' field" },
                            metadata = new { errorType = "InvalidRequest" },
                            location = location
                        }),
                        StatusCode = 400,
                        Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                    };
                }

                context.Logger.LogInformation($"Attempting to process request with path: {requestBody.path}");

                // Handle special case: "Start" - read and parse local index file
                if (requestBody.path.Equals("Start", StringComparison.OrdinalIgnoreCase))
                {
                    context.Logger.LogInformation("Processing 'Start' request - reading local index file");
                    
                    var parseResult = await ReadAndParseIndexFile(context);
                    
                    if (parseResult.success)
                    {
                        var location = await GetCallingIP();
                        
                        // Extract pdfCreation and metadata from the parsed result
                        dynamic indexData = parseResult.jsonData;
                        
                        // Log grouped information directly from the allFilenamesByBatchNumberAnditem_number data
                        try
                        {
                            var allFilenamesData = indexData.allFilenamesByBatchNumberAnditem_number as Dictionary<string, string[]>;
                            var groupedData = indexData.groupedByBatchNumberAnditem_number as Dictionary<string, dynamic>;
                            
                            if (allFilenamesData != null && groupedData != null)
                            {
                                foreach (var group in allFilenamesData)
                                {
                                    var key = group.Key;
                                    var allFiles = group.Value ?? Array.Empty<string>();
                                    
                                    // Get C and M file counts from the original grouped data
                                    var cFilenames = Array.Empty<string>();
                                    var mFilenames = Array.Empty<string>();
                                    
                                    if (groupedData.ContainsKey(key))
                                    {
                                        var groupValue = groupedData[key];
                                        cFilenames = groupValue?.cFilenames as string[] ?? Array.Empty<string>();
                                        mFilenames = groupValue?.mFilenames as string[] ?? Array.Empty<string>();
                                    }
                                    
                                    var fileList = string.Join(", ", allFiles);
                                    context.Logger.LogInformation($"Group {key}: [{fileList}] (C files: {cFilenames.Length}, M files: {mFilenames.Length})");
                                }
                            }
                            else
                            {
                                context.Logger.LogWarning("Failed to cast grouped data to expected types for logging");
                            }
                        }
                        catch (Exception logEx)
                        {
                            context.Logger.LogWarning($"Failed to log grouped info: {logEx.Message}");
                        }
                        
                        var body = new Dictionary<string, object>
                        {
                            { "pdfCreation", indexData.pdfCreation },
                            { "metadata", indexData.metadata },
                            { "location", location }
                        };

                        context.Logger.LogInformation("Successfully parsed local index file");

                        return new APIGatewayProxyResponse
                        {
                            Body = JsonSerializer.Serialize(body),
                            StatusCode = 200,
                            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                        };
                    }
                    else
                    {
                        context.Logger.LogError($"Failed to parse index file: {parseResult.error}");

                        var location = await GetCallingIP();
                        return new APIGatewayProxyResponse
                        {
                            Body = JsonSerializer.Serialize(new { 
                                pdfCreation = new { success = false, error = "Failed to read index file", details = parseResult.error },
                                metadata = new { errorType = "IndexFileParseError", path = requestBody.path },
                                location = location
                            }),
                            StatusCode = 500,
                            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                        };
                    }
                }

                // Download the index file from the provided path
                var downloadResult = await DownloadIndexFile(requestBody.path);

                if (downloadResult.success)
                {
                    var location = await GetCallingIP();
                    var body = new Dictionary<string, object>
                    {
                        { "pdfCreation", new { success = false, message = "No PDF creation performed - file downloaded only" } },
                        { "metadata", new { 
                            filePath = requestBody.path,
                            fileSize = downloadResult.size,
                            downloadedAt = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss UTC"),
                            contentLength = downloadResult.content?.Length ?? 0
                        } },
                        { "location", location }
                    };

                    context.Logger.LogInformation($"Successfully downloaded index file: {downloadResult.size} bytes");

                    return new APIGatewayProxyResponse
                    {
                        Body = JsonSerializer.Serialize(body),
                        StatusCode = 200,
                        Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                    };
                }
                else
                {
                    context.Logger.LogError($"Failed to download index file: {downloadResult.error}");

                    var location = await GetCallingIP();
                    return new APIGatewayProxyResponse
                    {
                        Body = JsonSerializer.Serialize(new { 
                            pdfCreation = new { success = false, error = "Failed to download index file", details = downloadResult.error },
                            metadata = new { errorType = "IndexFileDownloadError", path = requestBody.path },
                            location = location
                        }),
                        StatusCode = 502,
                        Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                    };
                }
            }
            catch (JsonException)
            {
                var location = await GetCallingIP();
                return new APIGatewayProxyResponse
                {
                    Body = JsonSerializer.Serialize(new { 
                        pdfCreation = new { success = false, error = "Invalid JSON in request body" },
                        metadata = new { errorType = "JsonParseError" },
                        location = location
                    }),
                    StatusCode = 400,
                    Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                };
            }
            catch (System.Exception ex)
            {
                context.Logger.LogError($"Error processing request: {ex.Message}");
                var location = await GetCallingIP();
                return new APIGatewayProxyResponse
                {
                    Body = JsonSerializer.Serialize(new { 
                        pdfCreation = new { success = false, error = "Internal server error", details = ex.Message },
                        metadata = new { errorType = "InternalServerError" },
                        location = location
                    }),
                    StatusCode = 500,
                    Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                };
            }
        }
    }
}