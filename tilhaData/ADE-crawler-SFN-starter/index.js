const AWS = require('aws-sdk');
const stepfunctions = new AWS.StepFunctions();
var util = require('util');


// Will be an S3 event
exports.handler = async (event, context) => {
    console.log(util.inspect(event, {depth: 5}));
    // Starter lambda response
    let response = {
        statusCode: 200,
        body: 'Change me'
    };
    
    //Step function params, input is the initial json 
    let params = {
      'stateMachineArn': process.env.SN_ARN, 
      'input': {} // parse S3 event information and other useful information
      // name: 'STRING_VALUE' <-- If needed the unique identifier can be self assigned, but we use an automatically generated
    };
    
    let srcBucket = event.Records[0].s3.bucket.name;
    let srcBucketARN = event.Records[0].s3.bucket.arn;
    let srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    let srcDate = event.Records[0].eventTime;
    
    let inputJson = { 'time':srcDate, 's3Bucket':srcBucket, 's3Key':srcKey };
    params.input = JSON.stringify(inputJson);
    
    console.log("## Params: " + util.inspect(params, {depth: 5}));

    return new Promise((resolve, reject) => {
        //Start the stepfunctions
        stepfunctions.startExecution(params, function(err, data) {
            if (err) {
                console.log(err, err.stack); // an error occurred
                reject(err);
            }
            else {
                console.log(data); // successful response
                response.body = JSON.stringify(data);
                resolve(response);
            }
        });
    });
};
