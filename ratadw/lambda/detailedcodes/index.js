'use strict'
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const https = require('https');
const waterfall = require('async-waterfall');

exports.handler = async (event) => {
    return new Promise((resolve, reject) => {
        console.log(JSON.stringify(`Event: event`))
        // Lambda Code Here
        waterfall([
            downloadCodes,
            saveJSON
        ], function(err, result){
            if (err) {
                console.log(err);
                reject(err);
            }
            else {
                console.log('## SUCCESS ');
                resolve(result);
            }
        })

        function downloadCodes(callback) {
            const options = {
                host: process.env.digitrafficHost,
                path: process.env.detailedcodesURL
            }
            
            console.log(options);

            https.get(options, (res) => {
                let vastaus = '';
                console.log('## statusCode:', res.statusCode);
                console.log('## headers:', res.headers);
    
                res.on('data', (d) => {
                    vastaus += d;
                });
                
                res.on('end', () => {
                    console.log('## END HTTP');
                    callback(null, vastaus);
                });
    
            }).on('error', (e) => {
                console.error(e);
                reject(e);
            });

            // end downloadCodes
        }

        function saveJSON(jsondata, callback) {
            console.log('## save JSON');
            console.log(jsondata);
            
            var buf = Buffer.from(jsondata, 'latin1');

            const bucketParams = {
                Bucket: process.env.workBucket,
                Key: process.env.prefix + '/' + process.env.prefix + '.json',
                ContentType: 'application/json',
                ContentLength: buf.length, 
                Body: buf 
            }
            
            console.log('## bucket params');
            console.log(bucketParams);

            s3.putObject(bucketParams, function(err, res) {
                if (err){  
                    console.log(err);
                    reject(err);
                } else {
                  console.log('## save json success!');
                  console.log('## ' + res);
                  callback(null, 'OK');
                }
            });

            // end saveJSON
        }

        // end promise
    })

    // end handler
}