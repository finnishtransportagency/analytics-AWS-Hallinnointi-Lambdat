'use strict'
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const waterfall = require('async-waterfall');
const jpath = require('jsonpath');

exports.handler = async (event) => {
    return new Promise((resolve, reject) => {
        console.log(JSON.stringify(`Event: event`))
        // Waterfall to keep cloud reads and writes in order
        waterfall([
            getThirdCategoryCodesFromS3,
            getDetailedCodesFromS3,
            getCauseCodesFromS3,
            combineCodes,
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

        function getThirdCategoryCodesFromS3(callback) {
            const options = {
                Bucket: process.env.workBucket,
                Key: process.env.thirdcategorycodesKey
            }
            
            console.log(options);

            s3.getObject(options, function(err, data){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## got third category codes');
                    //console.log(data);

                    var jsonObj = JSON.parse(data.Body.toString('latin1'));
                    callback(null, jsonObj);
                }
            })

            // end getThirdCategoryCodesFromS3
        }

        function getDetailedCodesFromS3(thirdcategory, callback) {
            console.log('## get detailed codes');
            //console.log(thirdcategory);

            const options = {
                Bucket: process.env.workBucket,
                Key: process.env.detailedcodesKey
            }
            
            console.log(options);

            s3.getObject(options, function(err, data){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## got detailed codes');
                    //console.log(data);

                    var jsonObj = JSON.parse(data.Body.toString('latin1'));

                    callback(null, jsonObj, thirdcategory);
                }
            })

            // end getDetailedCodesFromS3
        }

        function getCauseCodesFromS3(details, thirdcategory, callback) {
            console.log('## get cause codes');
            //console.log(details);

            const options = {
                Bucket: process.env.workBucket,
                Key: process.env.causecodesKey
            }
            
            console.log(options);

            s3.getObject(options, function(err, data){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## got cause codes');
                    //console.log(data);

                    var jsonObj = JSON.parse(data.Body.toString('latin1'));

                    callback(null, jsonObj, details, thirdcategory);
                }
            })

            // end getCodesFromS3
        }


        function combineCodes(causecodes, details, thirdcategory, callback) {
            let koodisto = [];

            console.log('## third category codes');
            thirdcategory.forEach(tccode => {
                //console.log(tccode);
                var ratadwsyykoodi = {};

                const tccodeId = tccode.thirdCategoryCode;
                //console.log(tccodeId);
                const dcode = jpath.query(details, '$[?(@.detailedCategoryCode=="' + tccodeId.substring(0,2) + '")]');
                //console.log(dcode);
                const ccode = jpath.query(causecodes, '$[?(@.categoryCode=="' + tccodeId.substring(0,1) + '")]');
                //console.log(ccode);

                //create a combined ratadwsyykodi from the above
                ratadwsyykoodi.syyn_aiheuttaja = 'n/a'; // where from?
                ratadwsyykoodi.syyluokka = ccode[0].categoryCode;
                ratadwsyykoodi.syykoodi = dcode[0].detailedCategoryCode;
                ratadwsyykoodi.tark_syykoodi = tccode.thirdCategoryCode;
                ratadwsyykoodi.syyluokka_selite = ccode[0].categoryName;
                ratadwsyykoodi.syykoodi_selite = dcode[0].detailedCategoryName;
                ratadwsyykoodi.tark_syykoodi_selite = tccode.thirdCategoryName;
                ratadwsyykoodi.tark_syy = tccode.thirdCategoryCode + ' ' + tccode.thirdCategoryName;

                //console.log(ratadwsyykoodi);
                koodisto.push(ratadwsyykoodi);
            });

            console.log('## koodisto valmis ' + koodisto.length);
            callback(null, koodisto);

            // end combineCodes
        }

        const CSV_SEPARATOR = ',';
        const CSV_LINE_BREAK = '\r\n';
        function createCSV(koodisto, callback){
            const csvheader = 'SYYN_AIHEUTTAJA,SYYLUOKKA,SYYKOODI,TARK_SYYKOODI,SYYLUOKKA_SELITE,SYYKOODI_SELITE,TARK_SYYKOODI_SELITE,TARK_SYY\r\n';
            let csvdata = '';
            csvdata += csvheader;
            koodisto.forEach(koodi => {
                var row = '';
                row = koodi.syyn_aiheuttaja + CSV_SEPARATOR +
                    koodi.syyluokka + CSV_SEPARATOR +
                    koodi.syykoodi + CSV_SEPARATOR +
                    koodi.tark_syykoodi + CSV_SEPARATOR +
                    '"'+koodi.syyluokka_selite+'"' + CSV_SEPARATOR +
                    '"'+koodi.syykoodi_selite+'"' + CSV_SEPARATOR +
                    '"'+koodi.tark_syykoodi_selite+'"' + CSV_SEPARATOR +
                    '"'+koodi.tark_syy+'"' + CSV_LINE_BREAK

                csvdata += row;
            });

            const csvbuffer = Buffer.from(csvdata,'latin1');
            const options = {
                Bucket: process.env.workBucket,
                Key: process.env.csvprefix,
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

            // end createCSV
        }

        // end promise
    })

    // end handler
}