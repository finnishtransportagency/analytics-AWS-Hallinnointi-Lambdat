# excel-to-csv-converter

## Prerequisites
Before building and using this Lambda, make sure you have the following installed:
- [Maven](https://maven.apache.org/) (for building the project)
- [Java](https://www.java.com/) (for running the Lambda)

## Building the Lambda
To build the Lambda, you can use either of the following scripts, depending on your operating system:

### Windows
Run the following command in a Command Prompt or PowerShell window:

**buildLambda.bat**

### Unix-like (Linux, macOS)
Run the following command in a terminal:

bash
**./buildLambda.sh**

## Using the Lambda
After building the Lambda, you can use the resulting JAR file located at:
**./excel-to-csv-converter/lambda/exceltocsv/target/excel-to-csv-s3-lambda-1.0.0.jar**

This JAR file can be deployed as code for your Lambda function.