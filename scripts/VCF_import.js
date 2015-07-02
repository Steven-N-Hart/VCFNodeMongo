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
var Header = []; ///depricate this
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
    var totalLines = 29; ///exec("wc -l " + filepath);
   // console.log(lineNum,totalLines);
    var lr = new LineByLineReader(filepath);
    lr.on('error', function (err) {
        console.error(err);
    }).on('end', function () {
        console.log("Line reading finished");
        //callback(); this emits before db operations are done...cannot close here.
    }).on('line', function (line) {
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
                    console.log(["linenum",lineNum,"totallines",totalLines]);
                    if (totalLines <= lineNum) {
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
                    orderedSampleids[i] = result[n]._id;
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
   /* var res = parseInputLine(line);
    if (res !== undefined) {    ///this is problem
        prepFormat(res, db);
    }*/
};



var findVariant = function(varObj, db, callback) {
    var collection = db.collection(config.names.variant);
    // Find this variant
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
            console.log("Var " + varObj.variant.chr + ":" + varObj.variant.pos + " " + varObj.variant.ref + "-" + varObj.variant.alt + "\tS=" + allSamples.length);
        }
        callback();
    });
};








//Async Code
var findDocument = function (OBJ, setQuery1, setQuery2, db) {
    // Get the documents collection
    var collection = db.collection('variants');

    // Insert some documents
    collection.findOne(
        {"chr": OBJ.chr, "pos": OBJ.pos, "ref": OBJ.ref, "alt": OBJ.alt},
        function (err, result) {
            assert.equal(err, null);
            console.log("result = " + JSON.stringify(result) + " typeof " + typeof result);
            if (result === null) {
                //console.log('Need to insert new record');
                OBJ.needsAnnotation = true;
                collection.insert(OBJ, function (err, result) {
                    assert.equal(err, null);
                    //console.log('Inserted '+ result);
                });
            }
            else {
                for (var j = 0; j < OBJ.samples.length; j++) {
                    //See if that sample already exists
                    var sampleExists = false;
                    for (var i = 0; i < result.samples.length; i++) {
                        if (result.samples[i].sample == OBJ.samples[j].sample) {
                            //console.log('Sample '+ OBJ.samples[j].sample+' already exists at position '+ i);
                            // Replace the samples[j] field
                            collection.updateOne(
                                {
                                    "chr": OBJ.chr,
                                    "pos": OBJ.pos,
                                    "ref": OBJ.ref,
                                    "alt": OBJ.alt,
                                    "samples.sample_id": OBJ.samples[j].sample_id,
                                    "samples.study_id": OBJ.samples[j].study_id
                                },
                                {"$set": setQuery2},
                                function (err2, results3) {
                                    //console.log('Situation3 = '+results3);
                                });
                            sampleExists = true;
                        }
                        ;
                    }//End i
                    // If you've gone through all of I and sampleExists is still false, push into array
                    if (sampleExists === false) {
                        //push sample data into the array
                        //console.log('I need to push this sample into the array')
                        collection.updateOne(
                            {"chr": OBJ.chr, "pos": OBJ.pos, "ref": OBJ.ref, "alt": OBJ.alt},
                            {"$push": {samples: setQuery1}},
                            function (err2, results4) {
                                // console.log('Situation4 = '+results4);
                            });
                    }
                }//End j
            }//End else
            //db.close();
        }
    )
};


// Prepare format for query
function prepFormat(res, db) {
    if (typeof res !== 'undefined') {
        var _id = res.shift();
        var PIECE1 = [];
        var PIECE2 = [];
        //var res = [];
        _id = _id._id;
        //console.log('res='+JSON.stringify(res));
        for (var j = 0; j < res.length; j++) {
            //console.log('sample='+res[j]['sample']);
            var line = {};
            line['chr'] = _id.chr;
            line['pos'] = Number(_id.pos);
            line['ref'] = _id.ref;
            line['alt'] = _id.alt;
            var SAMPLE = res[j]['sample_id'];
            var STUDY = res[j]['study_id'];

            // Now create set string for remaining variables
            delete res[j].study_id;
            delete res[j].sample_id;
            delete res[j]._id;
            //Get genotype count (GTC)
            var GTC = getGTC(res[j]['GT'])

            var setQuery1 = {'samples.$.GTC':GTC};
            var setQuery2 = {'GTC':GTC};
            for (var key in res[j]) {
                var attrName1 = key;
                var attrName2 = key;
                var attrValue1 = res[j][key];
                var attrValue2 = res[j][key];
                attrName1 = 'samples.$.' + attrName1;
                setQuery1[attrName1] = attrValue1;
                attrName2 = attrName2;
                setQuery2[attrName2] = attrValue2;
            }
            //Add in GenotTypeCount
            setQuery2['study_id'] = String(STUDY);
            setQuery2['sample_id'] = String(SAMPLE);
            //console.log('Q1: '+JSON.stringify(setQuery1));
            //console.log('Q2: '+JSON.stringify(setQuery2));
            PIECE2.push(setQuery2);
            PIECE1.push(setQuery1);
        } //End J
        line.samples = PIECE2;
        findDocument(line, PIECE1, PIECE2, db);
    } //End typeof
}


function parseInputLine(line) {
    var Variant = {};
        var Row = line.split('\t');
        var FORMAT = Row[8].split(':');
        var pos = Row[1];
        if (Row[4].match(',')) {
            console.log('I cannot handle multiple alleles');
            console.log(line);
        }
        else {
            var _id = {chr: Row[0], pos: Number(pos), ref: Row[3], alt: Row[4]};
            var lineArray = [{_id: _id}];
            for (var k = 9; k < Row.length; k++) {
                var sample_id = Header[k];
                var formatdata = {study_id: study_id, _id: _id};
                formatdata['sample_id'] = sample_id;
                //formatdata.sample = sample_id;
                if (!Row[k].match(/\.\/\./)) {
                    for (var j = 0; j < FORMAT.length; j++) {
                        //Create an array to store objects of SampleID and SampleFormat
                        var sampleFormat = Row[k].split(':');
                        var result = getValue(sampleFormat[j]);
                        //Set key-value pairs in formatdata
                        formatdata[FORMAT[j]] = result;
                    }//End format parsing
                } //End sample-level parsing
                //Only keep the samples with data
                if (typeof formatdata.GT !== 'undefined') {
                    lineArray.push(formatdata);
                }
            } // End All Samples
            //Insert my array into the complete object
            console.log(lineArray);
            return lineArray;
        }
}//end function


//Get number of alt genotypes
function getGTC(GT) {
    GT=JSON.stringify(GT)
    GT=GT.replace(/[\/\|\.0\"]/gi, '')
    return GT.length
}



function getValue(variable) {
    // for each value, convert to a number if it is one.
    var a = variable.split(',');
    //console.log("figuring out what to do with "+a);
    var result = '';
    if (a.length > 1) {
        result = numberOrStringMulti(a);
    }
    else {
        result = numberOrStringSingle(a);
    }
    return result;
}

function numberOrStringMulti(variable) {
    var result = variable.map(function (x) {
        if (isNaN(Number(x))) {
            if (x !== '.') {
                return String(x);
            }
        }
        else {
            return Number(x);
        }
    });
    return result;
}
function numberOrStringSingle(x) {
    var result = '';
    if (isNaN(Number(x))) {
        if (x !== '.') {
            result = String(x);
        }
    }
    else {
        result = Number(x);
    }
    return result;
}