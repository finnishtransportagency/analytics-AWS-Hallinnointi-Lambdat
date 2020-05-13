'use strict'
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const https = require('https');
const waterfall = require('async-waterfall');
const XLSX = require('xlsx');
const moment = require('moment');

exports.handler = async (event) => {
    return new Promise((resolve, reject) => {
        console.log(JSON.stringify(`Event: event`))
        // Lambda Code Here
        waterfall([
            readFile,
            parseExcel,
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

        function readFile(callback) {
            const options = {
                Bucket: process.env.excelbucket,
                Key: process.env.excelkey
            }
            
            console.log(options);

            s3.getObject(options, function(err, data){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## got excel data');
                    console.log(data);
                    callback(null, data.Body);
                }
                
            });
            // end readFile
        }

        function parseExcel(exceldata, callback){
            console.log('## parse excel data');
            console.log(exceldata, {type:"buffer"});
            let wb = XLSX.read(exceldata);
            console.log(wb);

            let koodit = [];
            wb.SheetNames.forEach(wsname => {
                let ws = wb.Sheets[wsname];
                //console.log(ws);
                //TODO: read cells H2 and I2 for validFrom and validTo dates
                let fromdatecell = ws['H2'];
                let todatecell = ws['I2'];

                console.log('## excel sheed valid date cells');
                console.log(fromdatecell);
                console.log(todatecell);

                // read 'formatted value' from .w, .v holds excel style date number
                let fromValue = (fromdatecell ? fromdatecell.w : undefined);
                let toValue = (todatecell ? todatecell.w : '01/01/2100'); // undefined value interpreted as valid until further notice

                console.log('## excel date cell values');
                console.log(fromValue);
                console.log(toValue);

                //FIX: is this a bug in xlsx module, or why the MM and DD have flipped places like this?
                let fromDate = moment(fromValue, 'MM/DD/YYYY');
                let toDate = moment(toValue, 'MM/DD/YYYY');

                console.log('## valid dates');
                console.log(fromDate.format());
                console.log(toDate.format());
                
                let jsonsheet = XLSX.utils.sheet_to_json(ws);
                console.log('## excel sheet to json');
                //console.log(jsonsheet);
                jsonsheet.forEach(row => {
                    Object.keys(row).forEach(key => {
                        let jsonrow = {"aiheuttaja" : "", "aiheuttajakoodi" : ""};
                        jsonrow.aiheuttaja = key.toString();
                        jsonrow.aiheuttajakoodi = row[key].toString();
                        jsonrow.validFrom = fromDate.format('YYYY-MM-DD');
                        jsonrow.validTo = toDate.format('YYYY-MM-DD');
                        //console.log(jsonrow);

                        koodit.push(jsonrow);
                    })
                })
                
            });

            callback(null, JSON.stringify(koodit));
        }

        function saveJSON(jsondata, callback) {
            console.log('## save JSON');
            console.log(jsondata);
            
            var buf = Buffer.from(jsondata);

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
            const csvheader = 'SYYNAIHEUTTAJA,SYYNAIHEUTTAJA_KOODI,ALKU_PVM,LOPPU_PVM'+CSV_LINE_BREAK;
            let csvdata = '';
            csvdata += csvheader;
            jsonObj.forEach(code => {
                csvdata += code.aiheuttaja + CSV_SEPARATOR +
                        '"'+code.aiheuttajakoodi+'"' + CSV_SEPARATOR +
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