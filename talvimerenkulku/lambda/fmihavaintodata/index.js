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
            if (err) {
                console.log(err);
                reject(err);
            }
            else {
                console.log('## SUCCESS ');
                resolve(result);
            }
        });

        // Ladataan FMI:n rajapinnan vastaus (XML) muistiin
        function downloadXML(callback){
            const options = {
                host: process.env.fmiHost,
                path: process.env.apiKey + process.env.havainnotURL
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
                    console.log('## END');
                    console.log(vastaus);
                    callback(null, vastaus);
                });
    
            }).on('error', (e) => {
                console.error(e);
                reject(e);
            });

        }

        // Muutetaan vastaus json muotoon ja tallennetaan valimuoto
        // s3 tyokansioon
        function saveJSON(xmldata, callback){
            // xml to json, drop name space for readability
            var options = {
                ignoreNameSpace : true
            };
            var jsonObj = parser.parse(xmldata ,options);
            console.log("to json -> %s", JSON.stringify(jsonObj));

            // save as json
            // not waiting for save result, already have the json for use
            // could change to async.waterfall
            var bucketParams = {
                Bucket: process.env.workBucket,
                Key: process.env.prefix + '/' + process.env.prefix + '.json',
                ContentType: 'application/json',
                ContentLength: JSON.stringify(jsonObj).length,
                Body: JSON.stringify(jsonObj)
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

        // Luodaan jsonin perusteella csv-rivit muistiin
        function createCSV(jsondata, callback) {
            // select location id
            let locationid = jp.query(jsondata, '$..Location.identifier');
            console.log('## location identifier: ');
            console.log(locationid);

            // select all the observations
            let measurements = jp.query(jsondata, '$..result.MeasurementTimeseries.point[*].MeasurementTVP');
            console.log('## measurements: ' + measurements.length);
            console.log(measurements);
            
            // start csv with a header
            let csvObj = 'FMISID,TIME,VALUE\r\n';

            // iterate observations for csv rows
            console.log('## creating csv rows');
            measurements.forEach(measurement => {
                csvObj = csvObj + locationid[0] + ',' + measurement.time + ',' + measurement.value + '\r\n';
            });
            console.log('## csv contents: ');
            console.log(csvObj);

            callback(null, csvObj);
        }
        
        // Tallennetaan csv annettuun s3 kansioon
        // TODO: ade manifest?
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
                    callback(null, data);
                    //resolve(data);
                }
            });
        }
    });
};
