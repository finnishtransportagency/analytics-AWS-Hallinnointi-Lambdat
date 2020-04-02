const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const https = require('https');
const moment = require('moment');
const waterfall = require('async-waterfall');

// reads a csv file from event and creates ADE manifest for it
// saves the manifest to ADE bucket
// copies csv file to ADE bucket
exports.handler = async (event) => {
    return new Promise((resolve, reject) => {
        const srcBucket = event.Records[0].s3.bucket.name;
        // Object key may have spaces or unicode non-ASCII characters.
        const srcKey    = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
        const timestamp = moment().valueOf();

        waterfall([
            readFile,
            createManifest,
            saveManifest,
            copyFiles2Ade
            ], function(err, result) {
                if (err) {
                    console.log(err);
                    reject(err);
                }
                else {
                    console.log('## SUCCESS ');
                    resolve(result);
                }
        });

        function readFile(callback){
            console.log('## get file');
            console.log(srcBucket);
            console.log(srcKey);
            s3.getObject({
                Bucket: srcBucket,
                Key: srcKey
            }, 
            callback); 
        }

        function createManifest(response, callback){
            const csvstring = response.Body.toString(); //buffer
            console.log('## manifest for csv');
            console.log(csvstring);

            const csvrows = csvstring.split('\r\n'); // ade style line feed
            console.log('## csv headers');
            console.log(csvrows[0]);

            const csvheaders = csvrows[0].split(',').map(h => h.trim());
            console.log(csvheaders);

            const manifestObj = {
                'entries': [{
                    'mandatory': true,
                    'url': ''
                }],
                'columns': []
            };

            // ade file naming conventions
            // skiph.1 = skip the first header line of the data file
            const srcfilenamearray = srcKey.split('/');
            let adefilename = srcfilenamearray[0] + '/' + 'table.' + srcfilenamearray[0] + '.' + timestamp + '.skiph.1' + '.csv';

            const manifesturl = 's3://' + process.env.adeBucket + '/' + adefilename;
            manifestObj.entries[0].url = manifesturl;
            manifestObj.columns = csvheaders;
            console.log('## manifest');
            console.log(manifestObj);

            callback(null, manifestObj);
        }

        function saveManifest(manifestdata, callback){
            const manifeststring = JSON.stringify(manifestdata);

            // ade file naming conventions
            // skiph.1 = skip the first header line of the data file
            const srcfilenamearray = srcKey.split('/');
            let ademanifestname = srcfilenamearray[0] + '/' + 'manifest-table.' + srcfilenamearray[0] + '.' + timestamp + '.skiph.1' + '.csv.json';
            
            // save as json
            const bucketParams = {
                Bucket: process.env.adeBucket,
                Key: ademanifestname,
                ContentType: 'application/json',
                ContentLength: manifeststring.length,
                Body: manifeststring
            }
            
            s3.putObject(bucketParams, function(err, data) {
                if (err){  
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## save manifest success!');
                    callback(null, data);
                }
            }); 
        }

        function copyFiles2Ade(copydata, callback){
            console.log('## copy files to ade');
            console.log(copydata);

            // ade file naming conventions
            // skiph.1 = skip the first header line of the data file
            const srcfilenamearray = srcKey.split('/');
            let adefilename = srcfilenamearray[0] + '/' + 'table.' + srcfilenamearray[0] + '.' + timestamp + '.skiph.1' +'.csv';
        
            // copy from work to ade
            const copyParams = {
                Bucket: process.env.adeBucket,
                Key: adefilename,
                CopySource: '/' + srcBucket + '/' + srcKey
            }

            console.log('## copy params');
            console.log(copyParams);

            s3.copyObject(copyParams, function(err, data){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## copy csv success!');
                    callback(null, data);
                }
            });
        }

    });
};
