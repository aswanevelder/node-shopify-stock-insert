// dependencies
const async = require('async');
//already installed on aws, not required as dependency in projects.json
const AWS = require('aws-sdk');
const util = require('util');
const eol = require("eol");
const MongoClient = require('mongodb').MongoClient;
const _ = require('lodash');
const environment = require('./environment');

// get reference to S3 client 
const s3 = new AWS.S3();

exports.handler = function (event, context, callback) {
    // Read options from the event.
    console.log("Reading options from event:\n", util.inspect(event, { depth: 5 }));
    const srcBucket = event.Records[0].s3.bucket.name;
    // Object key may have spaces or unicode non-ASCII characters.
    var srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    console.log("S3 Object key found: " + srcKey);

    // Download the image from S3, transform, and write to DynamoDB.
    async.waterfall([
        function(next) {
            // Download the csv file from S3 into a buffer. Create GroupId for file.
            s3.getObject({ Bucket: srcBucket, Key: srcKey }, next);
        },
        function(response, next) {
            const glueJsonString = response.Body.toString('utf-8');
            let lines = eol.split(glueJsonString);

            console.log("Lines found in file: " + lines.length);
            next(null, lines);
        },
        function(lines, next) {
           insertRecords(lines);
           next(null);
        }
    ], function (err) {
        if (err) {
            console.error(
                'Unable to put object' + srcBucket + '/' + srcKey +
                ' into ' + S3TOSERVICE_TOSERVICE + ' due to an error: ' + err
            );
        } else {
            console.log(
                'Successfully put object ' + srcBucket + '/' + srcKey +
                ' into ' + S3TOSERVICE_TOSERVICE 
            );
        }

        callback(null, "message");
    });
};

function insertRecords(lines) {
    MongoClient.connect(environment.STORE_DBURL, function (err, client) {
        if (err)
            console.error(err.message);

        const db = client.db(environment.STORE_DBNAME);
        const collection = db.collection(environment.STORE_COLLECTIONNAME);

        collection.deleteMany({}).then(async () => {
            try {
                console.log('Cleared Collection');
                const dataArray = createCollection(lines);
                const chunkArray = _.chunk(dataArray, 50000);
                
                for(let x=0; x < chunkArray.length; x++) {
                    const data = chunkArray[x]; 
                    console.log(`Try to insert collection ${x} with total records of ${data.length}`);
                    await collection.insertMany(data).then((result) => {
                        console.log("Inserted records: " + result.result.n);
                    }).catch((err) => console.log(err));
                }
            }
            catch(err) {
                console.log(err);
            }
        });
    });
}

function createCollection(lines) {
    let collection = [];
    let data;

    lines.forEach(line => {
        data = line.split(",");
        if (data.length === 5) {
            collection.push({
                "sku": parseInt(data[0], 10),
                "descr": data[1],
                "qty": data[2],
                "price": data[3],
                "promo": data[4]
            });
        }
    });

    console.log('Collection Count: ' + collection.length);
    return collection;
}
