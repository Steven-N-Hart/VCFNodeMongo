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

//var logger = require('./winstonLog');
//        ///   logger.info('log to file');

/*
 * Setup Commandline options, auto generates help
 */
CmdLineOpts
    .version('0.0.1')
    .usage('[options] -i <file> -n studyname')
    .option('-i, --input [file]', 'VCF file to be processed')
    .option('-n, --studyname [text]', 'Study Name, for importing')
    .parse(process.argv);
var ts1 = process.hrtime();


//Make sure are variables are set
if (!CmdLineOpts.input) {
  console.log("\nMissing Input VCF file.");
  CmdLineOpts.outputHelp();
  process.exit(27); // Exit Code 27: IC68342 = Missing Input Parameters
}

if (!CmdLineOpts.studyname) {
  console.log("\nMissing Study Name.");
  CmdLineOpts.outputHelp();
  process.exit(27);
}

//Get study ID
var study_id = CmdLineOpts.studyname;
var Header = []; ///depricate this
var sampleDbIds = [];


// Use connect method to connect to the Server
var url = 'mongodb://' + config.db.host + ':' + config.db.port + '/' + config.db.name;
MongoClient.connect(url, function (err, db) {
  assert.equal(null, err);
  //readMyFileLineByLine(db, CmdLineOpts.input, function () {
  getSampleInfoFromFile(db, CmdLineOpts.input, function () {
  // Time reporting - callback
    getDataFromFile(db, CmdLineOpts.input);
    var ts2 = process.hrtime(ts1);
    console.log('\n Total Time: %j s %j ms', ts2[0], (ts2[1] / 1000000));
    //db.close();
  });
});

/*###############################################################
 # Test new filereader
 ################################################################*/

var linediff = 0;
var numbInserted = 0;
var lineNum = 1;

//Get sample info
//Sychronus reading of file
var getSampleInfoFromFile  = function (db, filepath, callback) {
  var lr = new LineByLineReader(filepath);
  lr.on('error', function (err) {
    // 'err' contains error object
    console.log('Error reading file', err);
  });

  lr.on('line', function (line) {
    // 'line' contains the current line without the trailing newline character.
    if (line.match(/^#CHROM/)) {
      findSamples(db, line, function (ret) {
        sampleDbIds = ret;
        lr.close(); //Stop reading file.  This caused memory leak
        callback(db, filepath);
      });
    }
  });

  lr.on('end', function () {
    // All lines are read, file is closed now.
    console.log('Done reading file the first time');
  });
};



var fs = require('fs')
  , util = require('util')
  , stream = require('stream')
  , es = require("event-stream");

var getDataFromFile  = function (db, filepath) {
    var s;
    s = fs.createReadStream(filepath)
      .pipe(es.split())
      .pipe(es.mapSync(function (line) {
       // pause the readstream - if the memory leak comes back, uncomment this and the resume, but it slows things down (3X)
        //s.pause();


        //lineNum += 1;

        (function () {
            // process line here and call s.resume() when rdy
          if (!line.match(/^#/)) {
            processLines(line, lineNum, db, function () {
                //console.log(lineNum)
            });
          }
          // resume the readstream
          //s.resume(); //- if the memory leak comes back, uncomment this and the resume, but it slows things down
        })();
      })
        .on('error', function () {
          console.log('Error while reading file.');
        })
        .on('end', function () {
          console.log('Read entirefile.');
        })
        );
  }

/*###############################################################
 # Define functions
 ################################################################*/


// One database transaction for getting samples back...if all aren't already registered...accept none!
var findSamples = function (db, ln, callback) {
    var chromLine = ln.split('\t');
    var sampleNames = chromLine.slice(9, chromLine.length); // no need for a loop.
    var collection = db.collection(config.names.sample);
   //console.log({"study_id" : study_id, "sample_id" : { $in : sampleNames } });

    collection.find({"study_id" : study_id, "sample_id" : { $in : sampleNames } }).toArray(function (err, result) {
        //console.log('Array='+JSON.stringify(result))
        assert.equal(err, null, "Sample Check Issue");
        // returns empty, if study wrong, or no samples exist
        if (result.length < 1) {
            console.error("No Sample(s) in this study. Need to register all samples to '" + study_id + "' first.");
            process.exit(5);
        }
        if (sampleNames.length !== result.length) {
            console.error("All samples are not registered. VCF has: " + sampleNames.length + " DB has: " + result.length);
            process.exit(5);
        }

        var orderedSampleids = [];
        outerloop:
        for (var i in sampleNames){
            for (var n in result) {
                if( sampleNames[i] === result[n].sample_id ){
                    //console.log('ID='+result[n]._id.toHexString()+' n='+n+' i='+i)
                    orderedSampleids[i] = result[n]._id.toHexString(); //COnverted to hex string so its easier to seach later
                    continue outerloop;  // small loop efficiency
                }
            }
        }
        callback(orderedSampleids);
    });
};


//Process line
var processLines = function (line,lineNum, db, callback) {
    VariantRecord.parseVCFline(line, function(myVar){
        // file line is parsed into an object, now do database work
        //console.log('myVar='+JSON.stringify(myVar))
        //console.log(linediff)
        findVariant(myVar, db, function(ret) {
            updateVariant(myVar, ret, db, lineNum,function() {
                callback();
            });
        });
    });
};



var findVariant = function(varObj, db, callback) {
    var collection = db.collection(config.names.variant);
    //console.log('varObj.variant='+JSON.stringify(varObj.variant));   //varObj.variant,
   // var updateableVar = varObj.variant;
    //updateableVar['$setOnInsert'] = {needsAnnotation: true}; // {$setOnInsert: {needsAnnotation: true}}
    //console.log('varObj.variant='+JSON.stringify(varObj.variant));

   collection.findOneAndUpdate(varObj.variant, {$setOnInsert: {needsAnnotation: true}}, {upsert:true, returnOriginal:true}, function(err, found) {
        assert.equal(err, null);
        if (found.lastErrorObject.updatedExisting){
            //collection.update({_id:found.value._id, needsAnnotation: true}, function(err, fnd) {
                 //console.log("NEW! ",found.value);
                //This entry has been seen before
                callback({_id:found.value._id});
            //});
        }
        else{
            //This particular variant has not been seen before
            //console.log("Old! ",found.value);
            callback( {_id:found.lastErrorObject.upserted} );
        }
    });
};




var updateVariant = function(varObj, retVariant, db, lineNum,callback){
    var collection = db.collection(config.names.variant);
    var allSamples = [];// retVariant;
    //console.log('retVariant: '+JSON.stringify(retVariant)) //retVariant: {"_id":"55b1afbe3daa7a9f4c443850"}
    for (var h in varObj.sampleFormats){
        if (varObj.sampleFormats[h] === null){ continue; } //skip samples that don't carry this variant
        varObj.sampleFormats[h]['sample_id'] = sampleDbIds[h];
        allSamples.push(varObj.sampleFormats[h]);
    }
    //console.log('All Samples: '+ JSON.stringify(allSamples)) //[{"GT":"0|1","GQ":48,"DP":8,"HQ":[51,51],"GTC":1,"sample_id":"559a9bb5efeea832eafa8520"},{"GT":"0/1","GQ":43,"DP":5,"HQ":[null,null],"GTC":1,"sample_id":"559a9bb5efeea832eafa8521"}]
    /// This makes one query, updates any changes & inserts anything new
    //only load if there is a smple with a variant

    collection.update(retVariant,{$pushAll:{samples:allSamples}},{upsert:true,safe:false}, function (err,data) {
    if (err){ console.error(err); process.exit()}
    else{
        //numbInserted++;
        //linediff=lineNum - numbInserted
        //console.log("Var " + varObj.variant.chr + ":" + varObj.variant.pos + " " + varObj.variant.ref + ">" + varObj.variant.alt + "\tS=" + allSamples.length,'\tInserted='+numbInserted+'\tOn Line ='+lineNum,'\tDifference='+linediff);
    }
    callback();
    });
    
};


