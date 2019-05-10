const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const https = require('https');

exports.handler = async(event) => {
    // console.log("## ENVIRONMENT VARIABLES");
    // console.log(JSON.stringify(process.env, null, 2));
    // console.log("## EVENT");
    // console.log(JSON.stringify(event, null, 2));
    
    const username = process.env.sn_username
    const passw = process.env.sn_passw
    
    return new Promise((resolve, reject) => {
        const options = {
            host: process.env.sn_host,
            path: process.env.sn_path,
            headers: {
                'Authorization': 'Basic ' + new Buffer(username + ':' + passw).toString('base64')
            }
        }
        
        var bucketParams = {
            Bucket: process.env.bucket,
            Key: process.env.bucketKey,
            Body: ''
        }
        
        https.get(options, (res) => {
            let vastaus = '';
            console.log('## statusCode:', res.statusCode);
            console.log('## headers:', res.headers);

            res.on('data', (d) => {
                vastaus += d;
            });
            
            res.on('end', () => {
                console.log('## END');
                console.log(vastaus);
                bucketParams.Body = vastaus;
                s3.putObject(bucketParams, function(err, data) {
                    if (err){  
                        console.log(err);
                        reject(err);
                    } else {
                      console.log('## success!');
                      resolve(data);
                    }
                });
            });

        }).on('error', (e) => {
            console.error(e);
            reject(e);
        });
        
    });
};