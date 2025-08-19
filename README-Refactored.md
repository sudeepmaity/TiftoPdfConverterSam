# TiftoPdfConverterSam - Refactored Architecture

This project contains source code and supporting files for a refactored serverless application that processes ZIP files containing TIFF images and converts them to merged PDF documents using AWS Lambda, SQS, and S3.

## Architecture Overview

The application uses a **modular, event-driven architecture** with separate Lambda functions for different processing stages:

### Current Refactored Flow

1. **User uploads ZIP file** → S3 Source Bucket (zip/ folder)
2. **ZipExtractorFunction** (Lambda) extracts contents to S3 Source Bucket (extracted/ folder) and sends completion message to UnzipNotificationQueue (SQS)
3. **SQS event triggers GrouperFunction** (Lambda)
   - Groups TIFF files by pattern (batch_number + item_number)
   - Sends one SQS message per group to PdfCreationQueue
4. **New SQS Queue triggers CreateMergedPdfFunction** (Lambda)
   - Processes the TIFF group message
   - Creates merged PDF per group
   - Saves output to S3 Output Bucket (pdf/ folder)

### Key Components

- **src/ZipExtractorFunction/** - Extracts uploaded ZIP files
- **src/GrouperFunction/** - Groups TIFF files by pattern and sends group messages
- **src/CreateMergedPdfFunction/** - Creates merged PDFs from grouped TIFF files
- **template.yaml** - AWS SAM template defining all resources

## AWS Resources

- **2 Lambda Functions**: GrouperFunction, CreateMergedPdfFunction
- **2 SQS Queues**: UnzipNotificationQueue, PdfCreationQueue (FIFO)
- **2 S3 Buckets**: Source bucket (zip files + extracted content), Output bucket (PDFs)
- **Dead Letter Queues**: For failed message handling

## Key Improvements

### Modularity
- **Separation of Concerns**: Each function has a single responsibility
- **Independent Scaling**: Functions can scale independently based on workload
- **Easier Testing**: Each component can be tested in isolation

### Reliability
- **FIFO Queue**: Ensures ordered processing of groups
- **Dead Letter Queues**: Handles failed messages gracefully
- **Fine-grained Error Handling**: Better error isolation and recovery

### Performance
- **Parallel Processing**: Multiple PDF creation functions can run simultaneously
- **Smaller Function Size**: Faster cold start times
- **Resource Optimization**: Each function optimized for its specific task

## Deployment

The Serverless Application Model Command Line Interface (SAM CLI) is used for deployment:

```bash
sam build
sam deploy --guided
```

### Prerequisites

- SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
- .NET Core - [Install .NET Core](https://www.microsoft.com/net/download)
- Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

## Local Testing

Build your application:
```bash
sam build
```

Test individual functions:
```bash
# Test GrouperFunction
sam local invoke GrouperFunction --event events/unzip-notification.json

# Test CreateMergedPdfFunction  
sam local invoke CreateMergedPdfFunction --event events/tiff-group.json
```

Run the API locally:
```bash
sam local start-api
```

## Environment Variables

### GrouperFunction
- `SOURCE_BUCKET`: S3 bucket for extracted files
- `OUTPUT_BUCKET`: S3 bucket for final PDFs
- `OUTPUT_PREFIX`: Prefix for PDF files (default: "pdf")
- `PDF_CREATION_QUEUE_URL`: SQS queue URL for PDF creation requests

### CreateMergedPdfFunction
- `SOURCE_BUCKET`: S3 bucket for TIFF files
- `OUTPUT_BUCKET`: S3 bucket for final PDFs  
- `OUTPUT_PREFIX`: Prefix for PDF files (default: "pdf")

## Monitoring and Logging

- **CloudWatch Logs**: Each function has dedicated log groups
- **AWS X-Ray**: Distributed tracing enabled
- **Application Insights**: Automatic monitoring configuration

## File Processing Logic

### Grouping Strategy
Files are grouped by `batch_number` and `item_number` extracted from the index file:
- **Pattern**: `{batch_number}_{item_number}` 
- **File Types**: Both "C" (check) and "M" (memo) files are included in each group
- **Output**: One merged PDF per group containing all TIFF images

### Index File Format
The index file is expected to be a CSV with columns:
1. `batch_number` - Batch identifier
2. `item_number` - Item identifier  
3. `payment_type` - "C" or "M"
4. `file_name` - TIFF filename (without extension)

## Cleanup

To delete the application:
```bash
sam delete --stack-name TiftoPdfConverterSam
```

## Troubleshooting

### Common Issues

1. **FIFO Queue Configuration**: Ensure MessageGroupId and MessageDeduplicationId are properly set
2. **Aspose License**: Place license file in function directory for production use
3. **Memory Limits**: CreateMergedPdfFunction uses more memory (512MB) for PDF processing
4. **File Extensions**: Function automatically tries .tif, .tiff, .TIF, .TIFF extensions

### Monitoring

View function logs:
```bash
sam logs -n GrouperFunction --stack-name TiftoPdfConverterSam --tail
sam logs -n CreateMergedPdfFunction --stack-name TiftoPdfConverterSam --tail
```

## Resources

- [AWS SAM developer guide](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/what-is-sam.html)
- [AWS Lambda Documentation](https://docs.aws.amazon.com/lambda/)
- [Amazon SQS Developer Guide](https://docs.aws.amazon.com/sqs/)
- [Aspose.PDF Documentation](https://docs.aspose.com/pdf/net/)