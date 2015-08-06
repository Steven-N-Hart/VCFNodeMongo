/*jslint node: true */
/*jshint -W069 */
'use strict';

var CmdLineOpts = require('commander');
var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var config = require('./config.json');
var url = 'mongodb://' + config.db.host + ':' + config.db.port + '/' + config.db.name;

/*
 * Setup Commandline options, auto generates help
 */
CmdLineOpts
    .version('0.0.1')
    .usage('[options] -g groupName -o outname')
    .option('-o, --output [text]', 'Name of output file')
    .option('-g, --groupName [text]', 'Study Name, for grouping')
    .parse(process.argv);

var ts1 = process.hrtime();


//Make sure are variables are set
/*
if (!CmdLineOpts.output) {
    console.log('\nMissing output VCF file name');
    console.log('\tmultiple studynames are comma seperated');
    console.log('\nBy default, all samples in all studies are output\n');
    CmdLineOpts.outputHelp();
    process.exit(27); // Exit Code 27: IC68342 = Missing Input Parameters
}
*/
if (!CmdLineOpts.groupName) {
    console.log("\nMissing Group Name.");

    CmdLineOpts.outputHelp();
    process.exit(27);
}
//open the outfile
/*var fs = require('fs');
var stream = fs.createWriteStream(CmdLineOpts.output);
*/



///////////////////////////////////////
// Build functions

var groupName=CmdLineOpts.groupName
getSamples()


//Get the SampleID_ObjectID from the meta workspace
function getSamples() {
  MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);
    //console.log('Connected')
    var collection = db.collection('meta');

    var cursor=collection.aggregate(
       [
          {$group:{_id:"$"+groupName,samples:{$push:"$sample_id"}}}
       ]
    )
    cursor.on('data',function(data){
    console.log('data is '+data._id)


    })
  })
}

var createLoop <- function(data){
  var ID ={}
  ID['ID']=data._id
  
}