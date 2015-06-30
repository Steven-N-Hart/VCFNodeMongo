/*jslint node: true */
/*jshint -W069 */
'use strict';


var CmdLineOpts = require('commander');
var MongoClient = require('mongodb').MongoClient;
var LineByLineReader = require('line-by-line');
var assert = require('assert');
var config = require('./config.json');
var url = 'mongodb://' + config.db.host + ':' + config.db.port + '/' + config.db.name;

//Make sure are variables are set
if(typeof process.argv[2] === 'undefined') {
  console.log('Usage: Meta_import.js -i <tsv_file>');
  console.log('\nMust contain "study", "sample", and "kit" or else it will fail');
  console.log('\nAlso, the kit value needs to be registered before samples can be loaded');
  process.exit();
}



/*
 * Setup Commandline options, auto generates help
 */
CmdLineOpts
    .version('0.0.1')
    .usage('[options] -i <tsvfile>')
    .option('-i, --input [file]', 'TSV file to be uploaded')
    .parse(process.argv);

var ts1 = process.hrtime();


//Make sure are variables are set
if (!CmdLineOpts.input) {
    console.log("Missing Input Annotation file.");
    CmdLineOpts.outputHelp();
    process.exit(27); // Exit Code 27: IC68342 = Missing Input Parameters
}

//Get the header and make sure the samples, study, and kit ids are set
var fs = require('fs');
var entireFile = fs.readFileSync(CmdLineOpts.input,'utf8');
entireFile = entireFile.replace(/\r/g,'');

//lines is each line in the file
var lines = entireFile.split('\n');
var headerLine = [];
//Strip out any character that isnt a letter or number
lines[0]=lines[0].replace(/[^\w\s]/gi, '')
headerLine = lines[0].split('\t');


//check the file for required fields
var sampleIndex = headerLine.indexOf('sample_id');
var studyIndex = headerLine.indexOf('study_id');
var kitIndex = headerLine.indexOf('kit_id');

if (sampleIndex < 0 || studyIndex < 0 || kitIndex < 0){
	console.log('you are missing a required value in your meta file');
	console.log('sampleIndex='+sampleIndex);
	console.log('studyIndex='+studyIndex);
	console.log('kitIndex='+kitIndex);
	process.exit(2);
}

//Check the sample is registered
MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    //console.log('Connected')
    for (var i=1; i<lines.length;i++){
    	var newline = lines[i].split('\t');
        //Make sure evey sample has the required metadata
    	if (newline[sampleIndex] == null||newline[studyIndex] == null||newline[kitIndex] == null){
    		console.log('you are missing a required value in your meta file');
			console.log('sampleIndex='+sampleIndex);
			console.log('studyIndex='+studyIndex);
			console.log('kitIndex='+kitIndex);
			process.exit(1);
    	}
        console.log('NEWLINE2: '+ newline)
    	// Make sure the kit is registered
        var PASS = validateKit(db, newline[kitIndex] );
   		//Make a query and insert string to more easily manage code
   		var insertString ={};
   		var queryString={};
   		//get all the metadata elements from the file
   		for (var a=0; a<newline.length;a++){
   			insertString[headerLine[a]]=newline[a];
   		}
   		queryString['sample_id']=newline[sampleIndex];
   		queryString['study_id']=newline[studyIndex];
   		queryString['kit_id']=newline[kitIndex];
		var collection = db.collection('meta');
   		collection.update(
   			queryString,
   			insertString, {upsert:true},
   			function (err,res){
   				if(err){
   					console.log('Whooops.  THere was an error: '+err);
   				}
   				console.log('Success: '+JSON.stringify(res.result));
   			}
   		);
    	}//end for i loop
    //db.close();
    setTimeout(function(){db.close()}, 2000);
});

function validateKit(db, kit ){
    var collection = db.collection('kit_id');
    collection.findOne({'kit_id' : kit}, function (err,res){
        if(err || res === null){
            console.log('Error:'+err+' and res = '+res);
            process.exit(27);
        }
        else {
            return true;
        }        
    })
}