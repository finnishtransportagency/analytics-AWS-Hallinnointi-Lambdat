# AWS Java SDK project

Velhometadata lambda requests and saves Velho metadata from their API. To call the API the requests have to be signed with AWS v4 signature. Results are saved as is (json) with no document type transformations or compressing. Save location preserves the query path (ie. `/v1/nimikkeisto/varustetiedot/kaidetyyppi.json`) and is suitable for all Velho metadata queries.

This Lambda is scheduled with CloudWatch cron task.

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
 * List of Velho API endpoints is stored and managed in AWS Secret Manager