# Welcome to your CDK Java project!

You should explore the contents of this project. It demonstrates a CDK app with an instance of a stack (`VelhoAnalyticsStack`)
which contains an Amazon SQS queue that is subscribed to an Amazon SNS topic.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

It is a [Maven](https://maven.apache.org/) based project, so you can open this project with any Maven compatible Java IDE to build and run tests.

## Useful commands

 * `mvn package`     compile and run tests
 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

## Build and deploy
 * First build and package all the lambdas that need to be (re)delpoyed
    * This is done with `mvn package` in /lambdas directory (it has a parent project for all lambdas)
 * Then run `mvn package` in project root (here)
 * To deploy changes run `cdk deploy`in project root. You can specify the account by using your aws profiles aka `cdk deploy --profile vayla-ade-prod`

## Project structure

 * `src` has the cdk java classes and templates
    * `VelhoAnalyticsStack.java` for main resources and `PipleStack.java` for ci/cd stuff
 * `Cloudformation` has cloudformation templates for services that can't be done with cdk
 * `lambdas` holds the java code / projects for lambdas that contain all the business value of this project
