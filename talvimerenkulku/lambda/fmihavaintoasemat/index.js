const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const https = require('https');
const parser = require('fast-xml-parser');
const jp = require('jsonpath');
const waterfall = require('async-waterfall');

exports.handler = async (event) => {
    return new Promise((resolve, reject) => {
        
        waterfall([
            downloadXML,
            saveJSON,
            createCSV,
            saveCSV
            ], function(err, result) {
            // result now equals 'done'
            if (err) {
                console.log(err);
                reject(err);
            }
            else {
                console.log('## SUCCESS ');
                resolve(result);
            }
        });

        function downloadXML(callback){
            const options = {
                host: process.env.fmiHost,
                path: process.env.apiKey + process.env.havaintoasemaURL
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
                    console.log('## END of file');
                    console.log(vastaus);
                    callback(null, vastaus);
                });
    
            }).on('error', (e) => {
                console.error(e);
                reject(e);
            });
        }
        
        function saveJSON(jsondata, callback){
            // xml to json, drop name space for readability
            var options = {
                ignoreNameSpace : true
            };
            var jsonObj = parser.parse(jsondata ,options);

            // String for S3
            console.log("to json -> %s", JSON.stringify(jsonObj));
            var jsonString = JSON.stringify(jsonObj);

            // save as json
            // not waiting for save result, already have the json for use
            // could change to async.waterfall
            var bucketParams = {
                Bucket: process.env.workBucket,
                Key: process.env.prefix + '/' + process.env.prefix + '.json',
                ContentType: 'application/json',
                ContentLength: jsonString.length,
                Body: jsonString
            }
            s3.putObject(bucketParams, function(err, data) {
                if (err){  
                    console.log(err);
                    reject(err);
                } else {
                  console.log('## save json success!');
                  callback(null, jsonObj);
                }
            });
        }
        
        function createCSV(jsondata, callback) {
            // select all the stations
            let asemat = jp.query(jsondata, '$.FeatureCollection.member.EnvironmentalMonitoringFacility');
            console.log('## havaintoasemat: ' + asemat.length);
            console.log(asemat);
            
            // start csv with a header
            let csvObj = 'FMISID,NAME,POS_LAT,POS_LON\r\n';

            // iterate stations for csv rows
            console.log('## creating csv rows');
            asemat.forEach(asema => {
                const posarray = asema.representativePoint.Point.pos.split(' ');
                csvObj = csvObj + asema.identifier + ',' + asema.name[0] + ',' + posarray[0] + ',' + posarray[1] + '\r\n';
            });
            console.log('## csv contents: ');
            console.log(csvObj);
            callback(null, csvObj);
        }

        function saveCSV(csvdata, callback){
            // save as csv
            var bucketParams = {
                Bucket: process.env.workBucket,
                Key: process.env.prefix + '/' + process.env.prefix + '.csv',
                ContentType: 'text/csv',
                ContentLength: csvdata.length,
                Body: csvdata
            }
            
            s3.putObject(bucketParams, function(err, data) {
                if (err){  
                    console.log(err);
                    reject(err);
                } else {
                  console.log('## save csv success!');
                  //resolve(data);
                  callback(null, data);
                }
            }); 
        }
        
    });
};
