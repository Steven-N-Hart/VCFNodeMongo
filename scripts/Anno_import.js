/*jslint node: true */
/*jshint -W069 */
'use strict';

var CmdLineOpts = require('commander');
var MongoClient = require('mongodb').MongoClient;
var LineByLineReader = require('line-by-line');
var assert = require('assert');
var config = require('./config.json');
var VariantRecord = require('./VariantRecord.js');
var fs = require('fs');

var VariantRecord = require('./VariantRecord.js');
//var logger = require('./winstonLog');
//        ///   logger.info('log to file');

/*
 * Setup Commandline options, auto generates help
 */
CmdLineOpts
    .version('0.0.1')
    .usage('[options] -i <file> -n studyname')
    .option('-i, --input [file]', 'TSV file to be processed')
    .parse(process.argv);
var ts1 = process.hrtime();

//Make sure are variables are set
if (!CmdLineOpts.input) {
    console.log("\nMissing Input TSV file.");
    CmdLineOpts.outputHelp();
    process.exit(27); // Exit Code 27: IC68342 = Missing Input Parameters
}


var lr = new LineByLineReader(CmdLineOpts.input);
    lr
    .on('error', function (err) {
        console.error(err);
    })
    .on('line', function (line) {
    	if (line.match(/#CHROM/)){
    		validateInput(line,readAll)
    		lr.close()	
    	}
    	
    })

//Set global variable
var headerArray=[];


//Validate that POS, REF, and ALT are in thier correct positions
var validateInput = function (line,cb){
  	//Strip out any character that isnt a letter or number
	line=line.replace(/#CHROM/gi, 'chr')
	line=line.replace(/\./gi, '')
  	headerArray=line.split('\t')
  	var POSidx=headerArray.indexOf("POS");
  	var REFidx=headerArray.indexOf("REF");
  	var ALTidx=headerArray.indexOf("ALT");
  	//conver chr, pos, ref and alt to lowercase to match variant db
  	headerArray[0]=headerArray[0].toLowerCase();
	headerArray[1]=headerArray[1].toLowerCase();
	headerArray[2]=headerArray[2].toLowerCase();
	headerArray[3]=headerArray[3].toLowerCase();
  	if (POSidx !== 1 ||REFidx !== 2 || ALTidx !== 3){
  		console.log('The input file format is incorrect')
  		console.log('The header should start with #CHROM POS REF ALT ...')
  		process.exit(27)
  	}
  	//if it passes all these filters, then read the rest of the data
  	cb(line)
}

function readAll(line){
	var url = 'mongodb://' + config.db.host + ':' + config.db.port + '/' + config.db.name;
	MongoClient.connect(url, function (err, db) {
	assert.equal(null, err);	
	var lr2 = new LineByLineReader(CmdLineOpts.input);
    lr2
    .on('error', function (err) {
        console.error(err);
    })
    .on('end', function () {
        console.log("Line reading finished");
        // Time reporting - callback
       setTimeout(function(){
       		var ts2 = process.hrtime(ts1);
       		console.log('\n Total Time: %j s %j ms', ts2[0], (ts2[1] / 1000000));
       		db.close(); // This is closing too early so not all annotations are getting in
       },3000)

    })
    .on('line', function (line) {
    	if (!line.match(/#CHROM/)){
  				var lineArray=line.split('\t')
  				var lineObj=toObject(headerArray,lineArray)
  				//console.log(JSON.stringify(lineObj))
  				var queryObj={}
  				queryObj['chr']=lineObj['chr']
  				queryObj['pos']=numberOrStringSingle(lineObj['pos'])
  				queryObj['ref']=lineObj['ref']
  				queryObj['alt']=lineObj['alt']
  				delete lineObj['chr']
  				delete lineObj['pos']
  				delete lineObj['ref']
  				delete lineObj['alt']
  				mongoSet(db,queryObj,lineObj)
    	}
    })

	}
)}

function mongoSet(db,queryObj,lineObj){
	var collection = db.collection(config.names.variant);
	//console.log(JSON.stringify(queryObj))
	//console.log(JSON.stringify({annotation:lineObj}))
	collection.update(queryObj,{$set: {annotation:lineObj}},{upsert:true},
  	function(err, object) {
      if (err){
          console.warn(err.message);  // returns error if no matching object found
      }else{
          //console.dir(object);
      }
		})
}

//arr1 is the header array, arr2 is the line array
 function toObject(arr1,arr2) {
  var rv = {};
  for (var i = 0; i < arr1.length; ++i){
  	//skip blanks and '.' values
  	if(arr2[i] && !arr2[i].match(/^\.$/) ){
  		rv[arr1[i]]=arr2[i]
  	}
  }
  return rv;
}

var numberOrStringSingle = function (x) {
    var result = '';
    if (isNaN(Number(x))) {
        if (x !== '.') { result = String(x); }
    }
    else {
        result = Number(x);
    }
    return result;
};