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
            //seuraava paiva
            const begindate = moment().add(1, 'days').startOf('day').format('YYYY-MM-DDTHH:mm:ss');
            //fraktiilit 10pv kuten normaalit ennusteet tulevat fmi:lta
            const enddate = moment(begindate).add(9, 'days').startOf('day').format('YYYY-MM-DDTHH:mm:ss');
            console.log('## start date');
            console.log(begindate);
            console.log('## end date');
            console.log(enddate);

            //muutettu hakemaan maksimimaara ennusteita eika rajoiteta enaa 9 paivaan
            const dateparams = '&starttime=' + begindate; //+ '&endtime=' + enddate;
            const timesteps = 57; //14 pv + 1 timestep seuraavaan keskiyohon
            const timestepparams = '&timesteps=' + timesteps;

            const options = {
                host: process.env.fmiHost,
                path: process.env.apiKey + process.env.fraktiilitURL + dateparams + timestepparams
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

        function saveJSON(jsondata, callback){
            // xml to json, drop name space for readability
            const options = {
                ignoreNameSpace : true
            };
            const jsonObj = parser.parse(jsondata ,options);
            console.log("to json -> %s", JSON.stringify(jsonObj));

            // save as json
            const jsonString = JSON.stringify(jsonObj);
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
            const locationid = process.env.fmisid; // jp.query(jsondata, '$..Location.identifier'); --jos siis tulisi FMI:n vastauksessa, mutta kun ei tule
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
            const fractiles = JSON.stringify(datablock[0].doubleOrNilReasonTupleList).replace(/"/g, '').split(' \\n');
            console.log('## fractiles: ' + fractiles.length);
            console.log(fractiles);
            
            // start csv with a header
            let csvObj = 'FMISID,TIME,F10,F50,F90\r\n';
            console.log(csvObj);
            
            // iterate fractiles (sets) for csv rows
            console.log('## creating csv rows');
            let day = moment(begindate[0]);
            const stationid = process.env.fmisid;
            fractiles.forEach(fractileset => {
                console.group('## fractile set:' + fractileset);
                let fractilearray = fractileset.trim().split(' ');
                //todo: check that we got sane fractlearray with 3 cells?
                csvObj = csvObj + // header
                        stationid + ',' + //fmisid
                        day.format() + ',' + //time
                        fractilearray[0].trim() + ',' + //f10
                        fractilearray[1].trim() + ',' + //f50
                        fractilearray[2].trim() + //f90
                        '\r\n';
                day.add(6, 'hours'); // add 6 hours since we didnt get timevalue pairs, only start and enddate for whole set where 1 timestep = 6h
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
