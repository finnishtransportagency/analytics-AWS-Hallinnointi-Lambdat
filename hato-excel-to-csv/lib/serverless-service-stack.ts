import * as cdk from '@aws-cdk/core';
import * as iam from '@aws-cdk/aws-iam';
import { CfnOutput, Construct, DockerImage, Duration, RemovalPolicy, Resource, StackProps, Stage, Tag } from '@aws-cdk/core';
import s3 = require('@aws-cdk/aws-s3');
import * as lambda from '@aws-cdk/aws-lambda';
import { LambdaFunction } from '@aws-cdk/aws-events-targets';
import * as s3n from '@aws-cdk/aws-s3-notifications';
import { Rule, Schedule } from '@aws-cdk/aws-events';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import { SecretStringGenerator } from '@aws-cdk/aws-secretsmanager';

export class ServerlessServiceStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // Vain nimi, ensimmäinen osa
    var appname = this.stackName.split("-").slice(0)[0]
    // Vain env, viimeinen osa
    var env = this.stackName.split("-").slice(-1)[0]


    const lambdaRole = iam.Role.fromRoleArn(
      this,
      'imported-role-' + env,
      `arn:aws:iam::${cdk.Stack.of(this).account}:role/exceltocsvlambda-` + env,
      { mutable: true },
    );

    var arn = this.node.tryGetContext('ADE'+env+'ARN')

	
    // Lambda u_case
    datapipeExcelToCsv(
      this,               // construct
      appname,            // appname == "excelToCsv"
      env,                // env == dev|test|qa|prod
      this.region,				// region that is beign used
      lambdaRole,				  // role that allows cross region bucket put
      "com.cgi.lambda.exceltocsv.LambdaFunctionHandler",    //handler used in code
      "mvn clean install && cp ./target/excel-to-csv-s3-lambda-1.0.0.jar /asset-output/",    //buildcommand
      arn,                         // s3 output acl
      "file-load-ade-" + env,      // Fill in s3 output bucket
      "",                          // Fill in s3 output path
      "sftp-siirto-yhteinen-arkisto",     // Fill in s3 archive bucket
      "arkisto",                   // Fill in s3 archive path
      "TODO: target map"           // Fill in file map
      )

    
  }
}

function datapipeExcelToCsv(
  construct: cdk.Construct,
  appname: string,
  env: string,
  region: string,
  lambdaRole: iam.IRole,
  handler: string,
  buildcommand: string,
  output_arn: string,
  output_bucket: string,
  output_path: string,
  archive_bucket: string,
  archive_path: string,
  target_map: string
  ) {

  // ExcelToCsv-dev
  var functionName = appname + env

  const lambdaFunc = new lambda.Function(construct, functionName, {
    code: lambda.Code.fromAsset
      ("./lambda/exceltocsv/",
        {
          bundling:
            {
              command:
              ["/bin/sh", "-c", 
              buildcommand], 
              image: lambda.Runtime.JAVA_11.bundlingImage, 
              user: "root", 
              outputType: cdk.BundlingOutput.ARCHIVED
            }
      }
    ),
    functionName: functionName,
    handler: handler,
    runtime: lambda.Runtime.JAVA_11,
    timeout: Duration.minutes(15),
    memorySize: 2048,
    environment: {
      "add_path_ym": "true",
      "region": region,
      "output_arn": output_arn,
      "output_bucket": output_bucket,
      "output_path": output_path,
      "archive_bucket": archive_bucket,
      "archive_path": archive_path,
      "target_map": target_map
    },
    role: lambdaRole
  });
  

//  cdk.Tags.of(lambdaFunc).add("ExcelToCsv", sourcename)

}





