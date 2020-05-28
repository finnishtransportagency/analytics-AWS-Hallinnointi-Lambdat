# AWS Java SDK project

Velhocollector "collects" actual data (files) from Velho API. To call the API the requests have to be signed with AWS v4 signature. File is saved as is with no transformations or compressing.

Can be used to fetch any data file by changing the endpointurl environment variable of the Lambda.

It is a [Maven](https://maven.apache.org/) based project, so you can open this project with any Maven compatible Java IDE to build and run tests.

## Project structure
 * `src/main/java` - for business logic and signature stuff
 * `src/test/java` - for basic tests

## Build
 * `mvn package`

## Deploy
 * Installed and and redeployed normally from project root with `cdk deploy`
 * Can be deployed/updated with Eclipse aws plugin directly if needed
