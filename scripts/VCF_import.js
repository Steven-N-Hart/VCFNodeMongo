/*jslint node: true */
/*jshint -W069 */
'use strict';

var CmdLineOpts = require('commander');
var MongoClient = require('mongodb').MongoClient;
var LineByLineReader = require('line-by-line');
var assert = require('assert');
var config = require('./config.json');
var VariantRecord = require('./VariantRecord.js');
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
var sampleDbIds = [];


// Use connect method to connect to the Server
var url = 'mongodb://' + config.db.host + ':' + config.db.port + '/' + config.db.name;
MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);

    readMyFileLineByLine(db, CmdLineOpts.input, function () {
        // Time reporting - callback
        var ts2 = process.hrtime(ts1);
        console.log('\n Total Time: %j s %j ms', ts2[0], (ts2[1] / 1000000));
        db.close();
    });
});


/*###############################################################
 # Define functions
 ################################################################*/
var readMyFileLineByLine  = function (db, filepath, callback) {
    var lineNum = 1;
    var preLines = 1;
   // console.log(lineNum,totalLines);
    var lr = new LineByLineReader(filepath);
    lr.on('error', function (err) {
        console.error(err);
    }).on('end', function () {
        console.log("Line reading finished");
        //callback(); this emits before db operations are done...cannot close here.
    }).on('line', function (line) {
        preLines++;
        //console.log(["linenum",lineNum,"totallines",preLines]);
        // 'line' contains the current line without the trailing newline character.
        // skip lines here to avoid unnecessary callbacks
        if (!line.match(/^##/)) {
            if (line.match(/^#CHROM/)) {
                //Get sample index positions, must pause, need to have these processed before reading on.
                findSamples(db, line, function (ret) {
                    sampleDbIds = ret;
                    lineNum++;
                    lr.resume();
                });
                lr.pause();
            } else {
                processLines(line, db, function () {
                    lineNum++;
                    // sorta hacky....this is due to how  LineByLineReader emits events....may need to look at different file read strategy.
                    //console.log(["linenum",lineNum,"totallines",preLines]);
                    if (preLines <= lineNum) {
                        callback();
                    }
                });
               // console.log("Line ---- " + line);
            }
        }
        else { lineNum++; }
    });
};


// One database transaction for getting samples back...if all aren't already registered...accept none!
var findSamples = function (db, ln, callback) {
    var chromLine = ln.split('\t');
    var sampleNames = chromLine.slice(9, chromLine.length); // no need for a loop.
    var collection = db.collection(config.names.sample);
   // console.log({"study_id" : study_id, "sample_id" : { $in : sampleNames } });

    collection.find({"study_id" : study_id, "sample_id" : { $in : sampleNames } }).toArray(function (err, result) {
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
                    orderedSampleids[i] = result[n]._id.toHexString(); //COnverted to hex string so its easier to seach later
                    continue outerloop;  // small loop efficiency
                }
            }
        }
        callback(orderedSampleids);
    });
};


//Process line
var processLines = function (line, db, callback) {
    VariantRecord.parseVCFline(line, function(myVar){
        // file line is parsed into an object, now do database work
        findVariant(myVar, db, function(ret) {
            updateVariant(myVar, ret, db, function() {
                callback();
            });
        });
    });
};



var findVariant = function(varObj, db, callback) {
    var collection = db.collection(config.names.variant);

    collection.findOneAndUpdate(varObj.variant, varObj.variant, {upsert:true, returnOriginal:true}, function(err, found) {
        assert.equal(err, null);
        if (found.lastErrorObject.updatedExisting){
            callback({_id:found.value._id});
        }
        else{
            callback( {_id:found.lastErrorObject.upserted} );
        }
    });
};


var updateVariant = function(varObj, retVariant, db, callback){
    var collection = db.collection(config.names.variant);

    var allSamples = [];// retVariant;
    //allSamples['samples'] = [];
    for (var h in varObj.sampleFormats){
        if (varObj.sampleFormats[h] === null){ continue; } //skip samples that don't carry this variant
        varObj.sampleFormats[h]['sample_id'] = sampleDbIds[h];
        allSamples.push(varObj.sampleFormats[h]);
    }
    /// This makes one query, updates any changes & inserts anything new
    collection.update(retVariant,{ $pushAll:{samples:allSamples}},{upsert:true,safe:false}, function (err,data) {
        if (err){ console.error(err); }
        else{
            //console.log("Var " + varObj.variant.chr + ":" + varObj.variant.pos + " " + varObj.variant.ref + ">" + varObj.variant.alt + "\tS=" + allSamples.length);
        }
        callback();
    });
};



// Option for Find variant
/* collection.findOne(varObj.variant, function(err, found) {
 might be faster? i don't know
 /*
 if(found === null){
 collection.save(varObj.variant, function(err, doc){
 console.log("T3",process.hrtime());
 assert.equal(err, null);
 callback(doc);
 });
 }
 /// varObj.variant.needsAnnotation = true; There may be a better way to query this.

 console.log("T2",process.hrtime());
 callback(found);
 });*/
