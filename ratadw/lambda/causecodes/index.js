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
            checkDates,
            saveJSON,
            createCSV
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
                path: process.env.causecodesURL
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

        // HUOM! taalla muokataan alkuperaista dataa, eli lisataan toistaiseksi voimassa oleviin koodeihin 
        // kaukainen paivamaara 1.1.2100
        function checkDates(jsondata, callback) {
            let jsonobj = JSON.parse(jsondata);
            jsonobj.forEach(obj => {
                if(!obj.validTo) obj.validTo = '2100-01-01';
            })

            callback(null, JSON.stringify(jsonobj));
            
            //end checkDates
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
                  callback(null, jsondata);
                }
            });

            // end saveJSON
        }

        const CSV_SEPARATOR = ',';
        const CSV_LINE_BREAK = '\r\n';
        function createCSV(jsondata, callback) {
            let jsonObj = JSON.parse(jsondata);
            console.log('## create csv');
            const csvheader = 'SYYLUOKKA,SYYLUOKKA_SELITE,ALKU_PVM,LOPPU_PVM'+CSV_LINE_BREAK;
            let csvdata = '';
            csvdata += csvheader;
            jsonObj.forEach(code => {
                if(!code.validTo) code.validTo = '2100-01-01';
                csvdata += code.categoryCode + CSV_SEPARATOR +
                        '"'+code.categoryName+'"' + CSV_SEPARATOR +
                        code.validFrom + CSV_SEPARATOR +
                        code.validTo + CSV_LINE_BREAK;
            });

            const csvbuffer = Buffer.from(csvdata,'latin1');
            const options = {
                Bucket: process.env.workBucket,
                Key: process.env.prefix + '/' + process.env.prefix + '.csv',
                ContentType: 'text/csv',
                ContentLength: csvbuffer.length, 
                Body: csvbuffer 
            }

            s3.putObject(options, function(err, resp){
                if (err){  
                    console.log(err);
                    reject(err);
                } else {
                  console.log('## save csv success!');
                  console.log('## ' + resp);
                  callback(null, 'OK');
                }
            });

            // end create csv
        }

        // end promise
    })

    // end handler
}