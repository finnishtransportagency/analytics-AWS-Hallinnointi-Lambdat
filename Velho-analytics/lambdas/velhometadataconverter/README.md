# AWS Java SDK project

Velhometadataconverter converts the given data to ADE readable form and location. This includes json->csv transformation, creating a manifest file, and uploading the resulting files to ADE managed S3. Data file is saved with gzip compressing.

Json to csv transformation is mostly suitable for generic use. **Needs minor adjustements to naming/saving logic before generic use** 

Data is passed to this Lambda via S3 trigger.

It is a [Maven](https://maven.apache.org/) based project, so you can open this project with any Maven compatible Java IDE to build and run tests.

## Project structure
 * `src/main/java` - for business logic and signature stuff
    * Main logic is in **LambdaFunctionHandler.java** and in the **handleRequest** method
 * `src/test/java` - for basic tests

## Build
 * `mvn package`

## Deploy
 * Installed and and redeployed normally from project root with `cdk deploy`
 * Can be deployed/updated with Eclipse aws plugin directly if needed

## Other
 * CSV stuff is handled by on our own and relies on known model of the metadata (`Nimike.java`)
 * `NimikeCSVHelper.java` handles the mapping to csv format 
 * Lambda should send an alarm if recieved metadata version is different that expected (at the moment 1) 
 * Velho <-> ADE (filename) mapping is to be moved to AWS Secrets Manager