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
            getSourceCodesFromS3,
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

                    let jsonObj = JSON.parse(data.Body.toString('latin1'));
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

                    let jsonObj = JSON.parse(data.Body.toString('latin1'));

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

                    let jsonObj = JSON.parse(data.Body.toString('latin1'));

                    callback(null, jsonObj, details, thirdcategory);
                }
            })

            // end getCodesFromS3
        }

        function getSourceCodesFromS3(causecodes, details, thirdcategory, callback){

            console.log('## get sourcecodes');
            //console.log(details);

            const options = {
                Bucket: process.env.workBucket,
                Key: process.env.sourcecodesKey
            }
            
            console.log(options);

            s3.getObject(options, function(err, data){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## got cause codes');
                    //console.log(data);

                    let jsonObj = JSON.parse(data.Body.toString());

                    callback(null, jsonObj, causecodes, details, thirdcategory);
                }
            })

            // end getSourceCodesFromS3

        }


        // Yhdistaa eri tason koodit toisiinsa
        // 1) Koodin alkuosan perusteella
        // 2) Voimassaoloajan perusteella
        // Sama koodi voi siis esiintya omalla tasollaan useampaan otteeseen
        // mutta omalla voimassaoloajallaan ja todennakoisesti omalla selitteellaan
        function combineCodes(sourcecodes, causecodes, details, thirdcategory, callback) {
            let koodisto = [];

            console.log('## third category codes');
            thirdcategory.forEach(tccode => {
                //console.log(tccode);
                let ratadwsyykoodi = {};

                const scode = jpath.query(sourcecodes, '$[?(@.aiheuttajakoodi=="' + tccode.thirdCategoryCode + '")]');
                //console.log(scode);
                //tyhja jos syyn aiheuttajaa ei loydy kasin yllapidetysta excelista
                if(!scode || !scode[0]) {
                    scode[0] = {};
                }
                const tccodeId = tccode.thirdCategoryCode;
                //console.log(tccodeId);
                const codequery = '@.detailedCategoryCode=="' + tccodeId.substring(0,2) + '"';
                const datequery = ' @.validFrom <= "' + tccode.validFrom + '" && @.validTo >= "' + tccode.validTo + '"';
                let jpquery = '$[?(' + codequery + ' && ' + datequery + ')]';
                console.log('## jpquery ');
                console.log(jpquery);
                const dcode = jpath.query(details, jpquery);
                if(!dcode || !dcode[0]) {
                    dcode[0] = {};
                }
                //console.log(dcode);
                const causecodequery = '@.categoryCode=="' + tccodeId.substring(0,1) + '"';
                jpquery = '$[?(' + causecodequery + ' && ' + datequery + ')]';
                const ccode = jpath.query(causecodes, jpquery);
                if(!ccode || !ccode[0]) {
                    ccode[0] = {};
                }
                //console.log(ccode);

                //create a combined ratadwsyykoodi from all of the above
                ratadwsyykoodi.syyn_aiheuttaja = scode[0].aiheuttaja; 
                ratadwsyykoodi.syyluokka = ccode[0].categoryCode;
                ratadwsyykoodi.syykoodi = dcode[0].detailedCategoryCode;
                ratadwsyykoodi.tark_syykoodi = tccode.thirdCategoryCode;
                ratadwsyykoodi.syyluokka_selite = ccode[0].categoryName;
                ratadwsyykoodi.syykoodi_selite = dcode[0].detailedCategoryName;
                ratadwsyykoodi.tark_syykoodi_selite = tccode.thirdCategoryName;
                ratadwsyykoodi.tark_syy = tccode.thirdCategoryCode + ' ' + tccode.thirdCategoryName;
                ratadwsyykoodi.alku_pvm = tccode.validFrom;
                ratadwsyykoodi.loppu_pvm = tccode.validTo;

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
            const csvheader = 'SYYN_AIHEUTTAJA,SYYLUOKKA,SYYKOODI,TARK_SYYKOODI,SYYLUOKKA_SELITE,SYYKOODI_SELITE,TARK_SYYKOODI_SELITE,TARK_SYY,ALKU_PVM,LOPPU_PVM\r\n';
            let csvdata = '';
            csvdata += csvheader;
            koodisto.forEach(koodi => {
                let row = '';
                row = '"'+koodi.syyn_aiheuttaja+'"' + CSV_SEPARATOR +
                    '"'+koodi.syyluokka+'"' + CSV_SEPARATOR +
                    '"'+koodi.syykoodi+'"' + CSV_SEPARATOR +
                    '"'+koodi.tark_syykoodi+'"' + CSV_SEPARATOR +
                    '"'+koodi.syyluokka_selite+'"' + CSV_SEPARATOR +
                    '"'+koodi.syykoodi_selite+'"' + CSV_SEPARATOR +
                    '"'+koodi.tark_syykoodi_selite+'"' + CSV_SEPARATOR +
                    '"'+koodi.tark_syy+'"' + CSV_SEPARATOR +
                    '"'+koodi.alku_pvm+'"' + CSV_SEPARATOR +
                    '"'+koodi.loppu_pvm+'"' + CSV_LINE_BREAK;

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