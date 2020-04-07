const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const https = require('https');
const parser = require('fast-xml-parser');
const jp = require('jsonpath');
const moment = require('moment');
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
                path: process.env.apiKey + process.env.ennusteetURL
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

        function saveJSON(xmldata, callback){
            // xml to json, drop name space for readability
            const options = {
                ignoreNameSpace : true
            };
            const jsonObj = parser.parse(xmldata ,options);
            console.log("to json -> %s", JSON.stringify(jsonObj));

            const jsonString = JSON.stringify(jsonObj);
            // save as json string
            const bucketParams = {
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

        function createCSV(jsondata, callback){
            // select location id
            let locationid = jp.query(jsondata, '$..Location.identifier');
            console.log('## location identifier: ');
            console.log(locationid);

            // select begin and end date
            let begindate = jp.query(jsondata, '$..TimePeriod.beginPosition');
            console.log('## dates');
            console.log(begindate);

            // select the forecast(s)
            let datablock = jp.query(jsondata, '$..result.MultiPointCoverage.rangeSet.DataBlock');
            console.log('## datablock');
            console.log(datablock);
            const forecasts = JSON.stringify(datablock[0].doubleOrNilReasonTupleList).replace(/"/g, '').split(' \\n');
            console.log('## forecasts: ' + forecasts.length);
            console.log(forecasts);
            
            // start csv with a header
            let csvObj = 'FMISID,TIME,VALUE\r\n';

            // iterate observations for csv rows
            console.log('## creating csv rows');
            let day = moment(begindate[0]);
            const stationid = process.env.fmisid;
            forecasts.forEach(forecast => {
                csvObj = csvObj + stationid + ',' + day.format() + ',' + forecast.trim() + '\r\n';
                day.add(1, 'days'); // add a day since we didnt get timevalue pairs, only start and enddate for whole set
            });
            console.log('## csv contents: ');
            console.log(csvObj);
            callback(null, csvObj);
        }

        function saveCSV(csvdata, callback){
             // save as csv
            const bucketParams = {
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
