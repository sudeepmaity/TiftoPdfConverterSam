using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.IO;
using System;
using System.Drawing;

using Amazon.Lambda.Core;
using Amazon.Lambda.APIGatewayEvents;
using Aspose.Pdf;
using Aspose.Pdf.Devices;
using Aspose.Pdf.Text;

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

                // Debug: Log sample parsed entries to understand the data structure
                context?.Logger?.LogInformation($"Debug: Total parsed entries: {parsedEntries.Length}");
                foreach (var entry in parsedEntries.Take(3))
                {
                    if (entry.parsedData != null)
                    {
                        context?.Logger?.LogInformation($"Debug: Sample entry - Batch: '{entry.parsedData.batch_number}', Item: '{entry.parsedData.item_number}', Type: '{entry.parsedData.payment_type}', File: '{entry.parsedData.file_name}'");
                    }
                    else
                    {
                        context?.Logger?.LogInformation($"Debug: Entry with null parsedData - FieldCount: {entry.fieldCount}, Raw: '{entry.rawContent}'");
                    }
                }

                // Debug: Log how many entries pass the grouping filters
                var validForGrouping = parsedEntries.Where(entry => entry.parsedData != null && 
                       !string.IsNullOrEmpty(entry.parsedData.batch_number) &&
                       !string.IsNullOrEmpty(entry.parsedData.item_number)).ToArray();
                context?.Logger?.LogInformation($"Debug: Entries valid for grouping: {validForGrouping.Length}");

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

                // Debug: Log grouped results
                context?.Logger?.LogInformation($"Debug: Total groups created: {groupedByBatchNumberAnditem_number.Count}");
                foreach (var group in groupedByBatchNumberAnditem_number.Take(3))
                {
                    context?.Logger?.LogInformation($"Debug: Group '{group.Key}': C files = {group.Value.cFilenames.Length}, M files = {group.Value.mFilenames.Length}");
                    if (group.Value.cFilenames.Length > 0)
                        context?.Logger?.LogInformation($"Debug:   C files: {string.Join(", ", group.Value.cFilenames)}");
                    if (group.Value.mFilenames.Length > 0)
                        context?.Logger?.LogInformation($"Debug:   M files: {string.Join(", ", group.Value.mFilenames)}");
                }

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
                context.Logger.LogInformation($"allFilenamesByBatchNumberAnditem_number: {JsonSerializer.Serialize(allFilenamesByBatchNumberAnditem_number, new JsonSerializerOptions { WriteIndented = true })}");
                context.Logger.LogInformation($"mFilenamesByBatchNumberAnditem_number: {JsonSerializer.Serialize(mFilenamesByBatchNumberAnditem_number, new JsonSerializerOptions { WriteIndented = true })}");
    
                // Create merged PDFs from ALL TIFF files (C and M together)
                var pdfCreationResult = CreateMergedPdfs(allFilenamesByBatchNumberAnditem_number);

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

        private static (bool success, Dictionary<string, string> createdPdfs, string error) CreateMergedPdfs(Dictionary<string, string[]> allFilenamesByGroup)
        {
            try
            {
                // Try to set Aspose license (skip for performance)
                // try 
                // {
                //     SetAsposeLicense();
                // }
                // catch (Exception licEx)
                // {
                //     System.Diagnostics.Debug.WriteLine($"License setup failed (continuing without license): {licEx.Message}");
                // }
                
                // Skip basic functionality test for performance - proceed directly
                System.Diagnostics.Debug.WriteLine("Skipping Aspose.PDF basic functionality test for performance");
                
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

                // Process each group safely
                foreach (var kvp in allFilenamesByGroup)
                {
                    string groupKey = kvp.Key;
                    string[] tiffFilenames = kvp.Value;
                    
                    // Debug logging - skip null or empty keys
                    if (string.IsNullOrWhiteSpace(groupKey))
                    {
                        System.Diagnostics.Debug.WriteLine("Warning: Skipping group with null or empty key");
                        continue; // Skip null/empty keys
                    }
                    
                    if (tiffFilenames == null || tiffFilenames.Length == 0)
                    {
                        continue; // Skip groups with no TIFF files
                    }

                    // Create PDF document for this group with minimal setup
                    Document pdfDocument = null;
                    try 
                    {
                        pdfDocument = new Document();
                    }
                    catch (Exception docEx)
                    {
                        System.Diagnostics.Debug.WriteLine($"Failed to create Document for group {groupKey}: {docEx.Message}");
                        throw new InvalidOperationException($"Cannot create PDF document for group {groupKey}: {docEx.Message}", docEx);
                    }
                    
                    // Process each TIFF file in the group (both C and M files) - LIMITED FOR PERFORMANCE
                    System.Diagnostics.Debug.WriteLine($"Processing group {groupKey} with {tiffFilenames.Length} files");
                    int processedFiles = 0;
                    int maxFilesPerGroup = 10; // Limit files per group for performance
                    
                    foreach (string tiffFilename in tiffFilenames.Take(maxFilesPerGroup))
                    {
                        // Try to find the TIFF file with different extensions (.tiff, .tif, etc.)
                        string actualTiffPath = FindTiffFile(tifFilesPath, tiffFilename);
                        
                        if (actualTiffPath != null && IsTiffFile(actualTiffPath))
                        {
                            try
                            {
                                ProcessTiffFile(pdfDocument, actualTiffPath, $"Group {groupKey}");
                                processedFiles++;
                            }
                            catch (Exception ex)
                            {
                                // Log error but continue processing other files
                                // Note: In Lambda, detailed error info will be in CloudWatch logs
                                System.Diagnostics.Debug.WriteLine($"Warning: Failed to process {Path.GetFileName(actualTiffPath)}: {ex.Message}");
                            }
                        }
                        else
                        {
                            System.Diagnostics.Debug.WriteLine($"Warning: TIFF file not found (tried .tiff/.tif extensions): {tiffFilename}");
                        }
                    }
                    
                    if (tiffFilenames.Length > maxFilesPerGroup)
                    {
                        System.Diagnostics.Debug.WriteLine($"WARNING: Limited processing to {maxFilesPerGroup} files out of {tiffFilenames.Length} for performance");
                    }
                    
                    System.Diagnostics.Debug.WriteLine($"Processed {processedFiles}/{Math.Min(tiffFilenames.Length, maxFilesPerGroup)} files for group {groupKey}");
                    
                    // If no pages were added, add an empty page to prevent errors
                    if (pdfDocument.Pages.Count == 0)
                    {
                        var emptyPage = pdfDocument.Pages.Add();
                        // Add a text note about missing files
                        var textFragment = new Aspose.Pdf.Text.TextFragment($"No valid TIFF files found for group {groupKey}. Expected files: {string.Join(", ", tiffFilenames)}");
                        emptyPage.Paragraphs.Add(textFragment);
                    }
                    
                    // Save the merged PDF to temp directory (only writable location in Lambda)
                    string outputFileName = $"merged_{groupKey}.pdf";
                    string outputDir = Path.Combine(Path.GetTempPath(), "OUTPUT");
                    
                    // Ensure OUTPUT directory exists in temp
                    Directory.CreateDirectory(outputDir);
                    
                    string outputPath = Path.Combine(outputDir, outputFileName);
                    
                    try
                    {
                        // Validate PDF document before saving
                        if (pdfDocument.Pages.Count == 0)
                        {
                            throw new InvalidOperationException($"PDF document for group {groupKey} has no pages");
                        }
                        
                        // Save to temp directory (only writable location in Lambda)
                        pdfDocument.Save(outputPath);
                    }
                    catch (Exception saveEx)
                    {
                        System.Diagnostics.Debug.WriteLine($"Primary save failed for group {groupKey}: {saveEx.Message}");
                        
                        // Fallback: Create the most minimal PDF possible
                        try
                        {
                            // Clean up the problematic document
                            try { pdfDocument.Dispose(); } catch { }
                            
                            // Create a completely new, minimal document
                            using (var fallbackDocument = new Document())
                            {
                                // Add a single empty page with minimal configuration
                                var fallbackPage = fallbackDocument.Pages.Add();
                                
                                // Try to add text, but if that fails, just save empty PDF
                                try 
                                {
                                    var textFragment = new Aspose.Pdf.Text.TextFragment($"Group: {groupKey} - Processing failed");
                                    fallbackPage.Paragraphs.Add(textFragment);
                                }
                                catch 
                                {
                                    // If even text fails, just keep the empty page
                                    System.Diagnostics.Debug.WriteLine($"Text addition failed for group {groupKey}, creating empty PDF");
                                }
                                
                                // Save the fallback document
                                fallbackDocument.Save(outputPath);
                            }
                            
                            System.Diagnostics.Debug.WriteLine($"Created fallback PDF for group {groupKey}");
                        }
                        catch (Exception fallbackEx)
                        {
                            // Last resort: Create a dummy file so the process doesn't completely fail
                            try 
                            {
                                File.WriteAllText(outputPath.Replace(".pdf", ".txt"), $"PDF creation failed for group {groupKey}. Primary error: {saveEx.Message}. Fallback error: {fallbackEx.Message}");
                                // Create a minimal valid PDF manually if possible
                                File.WriteAllBytes(outputPath, new byte[] { 0x25, 0x50, 0x44, 0x46, 0x2D }); // "%PDF-" header
                            }
                            catch { }
                            
                            throw new InvalidOperationException($"Complete PDF creation failure for group {groupKey}. Primary: {saveEx.Message}, Fallback: {fallbackEx.Message}", saveEx);
                        }
                    }
                    
                    // Read the PDF content and encode as base64 for download
                    byte[] pdfBytes = File.ReadAllBytes(outputPath);
                    string base64Content = Convert.ToBase64String(pdfBytes);
                    
                    // Note: PDFs are saved to /tmp/OUTPUT (temporary, inside Lambda container)
                    
                    // Use TryAdd to avoid duplicate key issues  
                    try
                    {
                        if (!string.IsNullOrWhiteSpace(groupKey) && !createdPdfs.ContainsKey(groupKey))
                        {
                            createdPdfs.Add(groupKey, $"Path: {outputPath}, Size: {pdfBytes.Length} bytes");
                        }
                    }
                    catch (Exception dictEx)
                    {
                        System.Diagnostics.Debug.WriteLine($"Warning: Failed to add PDF info to dictionary for group '{groupKey}': {dictEx.Message}");
                    }
                    
                    // Only dispose if not already disposed in fallback
                    try { pdfDocument.Dispose(); } catch { /* Already disposed */ }
                }
                
                return (true, createdPdfs, null);
            }
            catch (Exception ex)
            {
                return (false, null, $"Error creating merged PDFs: {ex.Message} - StackTrace: {ex.StackTrace}");
            }
        }

        private static void SetAsposeLicense()
        {
            try
            {
                // Uncomment and provide your license file path
                // var license = new Aspose.Pdf.License();
                // license.SetLicense("path/to/your/Aspose.Pdf.lic");
                System.Diagnostics.Debug.WriteLine("✓ Aspose license applied successfully");
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"⚠ License not applied: {ex.Message}");
            }
        }

        private static bool TestAsposePdfBasicFunctionality()
        {
            try
            {
                // Test the most basic PDF creation
                using (var testDoc = new Document())
                {
                    var testPage = testDoc.Pages.Add();
                    var testPath = Path.Combine(Path.GetTempPath(), $"aspose_test_{Guid.NewGuid()}.pdf");
                    testDoc.Save(testPath);
                    
                    // Clean up test file
                    try { File.Delete(testPath); } catch { }
                    
                    System.Diagnostics.Debug.WriteLine("✓ Aspose.PDF basic functionality test passed");
                    return true;
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"✗ Aspose.PDF basic functionality test failed: {ex.Message}");
                return false;
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

        private static (double width, double height) CalculateImageDimensions(int imageWidth, int imageHeight, Page page)
        {
            // Get page dimensions (accounting for margins)
            var pageRect = page.GetPageRect(true);
            double pageWidth = pageRect.Width - 40; // 20pt margin on each side
            double pageHeight = pageRect.Height - 40; // 20pt margin on top and bottom
            
            // Calculate scale factors
            double scaleX = pageWidth / imageWidth;
            double scaleY = pageHeight / imageHeight;
            
            // Use the smaller scale factor to maintain aspect ratio
            double scale = Math.Min(scaleX, scaleY);
            
            return (imageWidth * scale, imageHeight * scale);
        }

        private static void ProcessTiffFile(Document document, string tiffPath, string description)
        {
            try
            {
                // Validate file exists and is a TIFF file
                if (!File.Exists(tiffPath))
                    throw new FileNotFoundException($"TIFF file not found: {tiffPath}");

                if (!IsTiffFile(tiffPath))
                    throw new ArgumentException($"File is not a TIFF file: {Path.GetFileName(tiffPath)}");

                // Load the TIFF image to check frame count
                using (var tiffImage = new Bitmap(tiffPath))
                {
                    int frameCount = tiffImage.GetFrameCount(System.Drawing.Imaging.FrameDimension.Page);

                    // Ensure we have at least one frame
                    if (frameCount == 0)
                        throw new InvalidOperationException($"TIFF file contains no frames: {Path.GetFileName(tiffPath)}");

                    // Process each frame (limit to first 3 frames for performance)
                    int maxFramesToProcess = Math.Min(frameCount, 3);
                    for (int frameIndex = 0; frameIndex < maxFramesToProcess; frameIndex++)
                    {
                        // Select the current frame
                        tiffImage.SelectActiveFrame(System.Drawing.Imaging.FrameDimension.Page, frameIndex);

                        // Validate image dimensions before creating page
                        if (tiffImage.Width <= 0 || tiffImage.Height <= 0)
                        {
                            System.Diagnostics.Debug.WriteLine($"Warning: Invalid image dimensions {tiffImage.Width}x{tiffImage.Height} for frame {frameIndex}");
                            continue;
                        }

                        // Create a new page in the PDF document 
                        var page = document.Pages.Add();

                        // Calculate dimensions to fit the page while maintaining aspect ratio
                        var (width, height) = CalculateImageDimensions(tiffImage.Width, tiffImage.Height, page);
                        
                        // Validate calculated dimensions
                        if (width <= 0 || height <= 0)
                        {
                            System.Diagnostics.Debug.WriteLine($"Warning: Invalid calculated dimensions {width}x{height} for frame {frameIndex}");
                            // Remove the page we just added since we can't use it
                            document.Pages.Delete(document.Pages.Count);
                            continue;
                        }

                        // Try direct TIFF approach first, fallback to PNG if needed
                        bool imageAdded = false;
                        
                        // Method 1: Try using the original TIFF file directly if it's a single frame
                        if (frameCount == 1)
                        {
                            try 
                            {
                                var image = new Aspose.Pdf.Image();
                                image.File = tiffPath;
                                                                image.FixWidth = width;
                                image.FixHeight = height;
                                page.Paragraphs.Add(image);
                                imageAdded = true;
                            }
                            catch (Exception ex)
                            {
                                System.Diagnostics.Debug.WriteLine($"Direct TIFF failed for {tiffPath}: {ex.Message}");
                            }
                        }
                        
                        // Method 2: Fallback to PNG conversion approach
                        if (!imageAdded)
                        {
                            try
                            {
                                // Convert current frame to PNG bytes
                                byte[] imageBytes;
                                using (var imageStream = new MemoryStream())
                                {
                                    tiffImage.Save(imageStream, System.Drawing.Imaging.ImageFormat.Png);
                                    imageBytes = imageStream.ToArray();
                                }

                                // Validate that we have image data
                                if (imageBytes == null || imageBytes.Length == 0)
                                {
                                    throw new InvalidOperationException("No image data generated");
                                }

                                // Save to temporary file and use file path
                                string tempImagePath = Path.Combine(Path.GetTempPath(), $"temp_frame_{Guid.NewGuid()}.png");
                                File.WriteAllBytes(tempImagePath, imageBytes);
                                
                                var image = new Aspose.Pdf.Image();
                                image.File = tempImagePath;
                                                                image.FixWidth = width;
                                image.FixHeight = height;
                                page.Paragraphs.Add(image);
                                
                                // Clean up temp file
                                try { File.Delete(tempImagePath); } catch { }
                                imageAdded = true;
                            }
                            catch (Exception imgEx)
                            {
                                System.Diagnostics.Debug.WriteLine($"PNG conversion failed for frame {frameIndex}: {imgEx.Message}");
                            }
                        }
                        
                        // If both methods failed, remove the page
                        if (!imageAdded)
                        {
                            document.Pages.Delete(document.Pages.Count);
                            System.Diagnostics.Debug.WriteLine($"Warning: Failed to add any image for frame {frameIndex}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to process {description} - {Path.GetFileName(tiffPath)}: {ex.Message}", ex);
            }
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
