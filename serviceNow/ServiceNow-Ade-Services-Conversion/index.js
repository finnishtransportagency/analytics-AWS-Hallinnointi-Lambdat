const async = require('async');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();

exports.handler = async(event) => {
    console.log('## EVENT');
    console.log(JSON.stringify(event, null, 2));
    // otetaan eventista talteen tarvittavat tiedot
    // kuten esim bucketin ja tiedoston nimi
    // let srcBucket = event.Records[0].s3.object.bucket.name;
    let srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));

    // tarkistetaan tiedostotyyppi
    let typeMatch = srcKey.match(/\.([^.]*)$/);
    if (!typeMatch) {
        console.log('## Tiedostotyyppia ei voitu lukea');
        return new Promise((resolve, reject) => {
            let err = new Error('Tiedostotyyppia ei voitu lukea');
            reject(err);
        });
    }

    let fileType = typeMatch[1];
    if (fileType != 'json') {
        console.log('## Ei tuettu tiedostomuoto: ' + fileType);
        return new Promise((resolve, reject) => {
            let err = new Error('Ei tuettu tiedostomuoto: ' + fileType);
            reject(err);
        });
    }
    
    // otetaan tiedoston nimi talteen, silla sita kaytetaan s3 bucketeissa prefixina
    // voisi olla ymparistomuuttujakin, mutta nain sailyy jonkinlainen yleiskayttoisuus
    // eri nimisille json tiedostoille muuttamalla tiedoston nimea ainoastaan
    // tiedostonhakulambdassa
    let nameMatch = srcKey.match(/(.+?)(\.[^.]*$|$)/);
    if (!nameMatch) {
        console.log('## Tiedoston nimea ei voitu lukea');
        return new Promise((resolve, reject) => {
            let err = new Error('Tiedoston nimea ei voitu lukea');
            reject(err);
        });
    }
    
    let fileName = nameMatch[1];
    
    const time = Date.now();

    console.log('## Aloitetaan tiedoston kasittely: ' + srcKey + 'tiedoston nimi; ' + fileName +', tiedostomuoto: ' + fileType);

    return new Promise((resolve, reject) => {
        // async waterfall, missa
        // 0. ladataan alkuperainen tiedosto muistiin
        // 1. kopioidaan alkuperainen tiedosto ade-buckettiin
        // 2. luodaan manifestin sisalto
        // 3. tallennetaan manifest sille kuuluvaan aden manifest buckettiin
        // jatkokehitys: siivotaan aiempien vaihdeiden tulokset (tiedostot),  
        // mikali myohemmassa vaiheessa tormataan virheeseen 
        async.waterfall([
            downloadFileFunction,
            uploadFileFunction,
            createManifestFunction,
            uploadManifestFunction,
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

        function downloadFileFunction(callback) {
            console.log('## start download');
            s3.getObject({
                Bucket: process.env.srcBucket,
                Key: srcKey
            }, callback);
        }

        // generoidaan tiedoston nimi sis. aikaleima
        function uploadFileFunction(data, callback) {
            console.log('## start upload');
            console.log('## download data length: ' + data.Body.length);

            let destKey = '';
            destKey = fileName + '/table.' + fileName + '.' + time + '.batch.' + time + '.fullscanned.true.json';

            console.log('## tallennusavain: ' + destKey);

            s3.putObject({
                Bucket: process.env.destBucket,
                Key: destKey,
                Body: data.Body,
                ACL: 'bucket-owner-full-control'
            }, function(err, response){
                if(err) {
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## put json response: ' + JSON.stringify(response));
                    callback(null, response, destKey);
                }
            });
        }

        // luodaan manifest ja manifest tiedoston nimi
        function createManifestFunction(s3Response, destKey, callback) {
            console.log('## start manifest creation');
            let manifestJson = {
                'entries': [

                ],
                'columns': [
                    'DATA'
                ]
            };

            let manifestEntry = {
                'mandatory': 'true',
                'url': 's3://' + process.env.destBucket + '/' + destKey
            };

            manifestJson.entries.push(manifestEntry);
            console.log('## manifest created: ');
            console.log(JSON.stringify(manifestJson));
            
            callback(null, manifestJson);
        }

        // tallennetaan manifest ADE:n omaan s3 buckettiin
        function uploadManifestFunction(manifestdata, callback) {
            console.log('## start manifest upload');
            
            // luodaan kohdenimi saman tien tyhjasta, silla nimeamislogiikka on jostain
            // syysta erilainen paatiedostoon nahden jopa keskella nimea
            const destKey = process.env.destBucketManifestPrefix + 'manifest-table.servicenow_' + fileName + '.' + time + '.batch.' + time + '.fullscanned.true.json';
            console.log('## manifest destkey: ' + destKey);
            
            s3.putObject({
                Bucket: process.env.destBucketManifest,
                Key: destKey,
                Body: JSON.stringify(manifestdata),
                ACL: 'bucket-owner-full-control'
            }, function(err, response){
                if(err){
                    console.log(err);
                    reject(err);
                } else {
                    console.log('## s3 put manifest response: ' + JSON.stringify(response));
                    callback(null, 'done');
                }
            });
        }
    });

};