/*jslint node: true */
/*jshint -W069 */
'use strict';


var MongoClient = require('mongodb').MongoClient;
var assert = require('assert');
var url = 'mongodb://localhost:27017/dev';

//Make sure are variables are set
if(typeof process.argv[2] === 'undefined') {
  console.log('Usage: Meta_import.js <tsv_file>');
  console.log('\nMust contain "study", "sample", and "kit" or else it will fail');
  console.log('\nAlso, the kit value needs to be registered before samples can be loaded');
  process.exit();
}

//Get the header and make sure the samples, study, and kit ids are set
var fs = require('fs');
var entireFile = fs.readFileSync(process.argv[2],'utf8');
entireFile = entireFile.replace(/\r/g,'');

//lines is each line in the file
var lines = entireFile.split('\n');
var headerLine = [];
headerLine = lines[0].split('\t');

//check the file for required fields
var sampleIndex = headerLine.indexOf('sample');
var studyIndex = headerLine.indexOf('study');
var kitIndex = headerLine.indexOf('kit');

if (sampleIndex < 0 || studyIndex < 0 || kitIndex < 0){
	console.log('you are missing a required value in your meta file');
	console.log('sampleIndex='+sampleIndex);
	console.log('studyIndex='+studyIndex);
	console.log('kitIndex='+kitIndex);
	process.exit();
}

//Check the sample is registered
MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    //console.log('Connected')
    for (var i=1; i<lines.length;i++){
    	var newline = lines[i].split('\t');
    	if (newline[sampleIndex] == null||newline[studyIndex] == null||newline[kitIndex] == null){
    		console.log('you are missing a required value in your meta file');
			console.log('sampleIndex='+sampleIndex);
			console.log('studyIndex='+studyIndex);
			console.log('kitIndex='+kitIndex);
			process.exit();
    	}
    	// Make sure the kit is registered
   	    var collection = db.collection('kit');
    	collection.findOne({'kit' : newline[kitIndex]}, function (err,res){
    		if(err || res === null){
    			console.log('Error:'+err);
    			process.exit();
    		}

    		//Make a query and insert string to more easily manage code
    		var insertString ={};
    		var queryString={};
    		//get all the metadata elements from the file
    		for (var a=0; a<newline.length;a++){
    			insertString[headerLine[a]]=newline[a];
	   		}
	   		queryString['sample']=newline[sampleIndex];
	   		queryString['study']=newline[studyIndex];
	   		queryString['kit']=newline[kitIndex];

	   		//console.log('querying:'+JSON.stringify(queryString));
    		//console.log('inserting:'+JSON.stringify(insertString))
			collection = db.collection('meta');
    		collection.update(
    			queryString,
    			insertString, {upsert:true},
    			function (err,res){
    				if(err){
    					console.log(err);
    				}
    				console.log(res.result);
    			}
    		);
    	});
    }//end for i loop
    //db.close();
});

