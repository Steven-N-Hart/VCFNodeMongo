/*jslint node: true */
/*jshint -W069 */
'use strict';

var CmdLineOpts = require('commander');
var MongoClient = require('mongodb').MongoClient;
var LineByLineReader = require('line-by-line');
var assert = require('assert');
var config = require('./config.json');
var url = 'mongodb://' + config.db.host + ':' + config.db.port + '/' + config.db.name;

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
//Call the main DB to make sure it exists and get its uniq id and get its KitID
var Header = [];
var sampleNames = [];


// Use connect method to connect to the Server
MongoClient.connect(url, function (err, db) {
    assert.equal(null, err);

    readMyFileLineByLine(db, CmdLineOpts.input, function () {
        // Time reporting - callback
        var ts2 = process.hrtime(ts1);
        console.log('\n Total Time: %j s %j ms', ts2[0], (ts2[1] / 1000000));
       //db.close();
        setTimeout(function(){db.close()}, 2000);
    });
});


/*###############################################################
 # Define functions
 ################################################################*/

var readMyFileLineByLine  = function(db, filepath, callback) {
    var lr = new LineByLineReader(filepath);
    lr.on('error', function (err) {
        console.error(err);
    }).on('end', function () {
        console.log("Line reading finished");
        callback();
    }).on('line', function (line) {
        // 'line' contains the current line without the trailing newline character.
        processLines(line, db);
        //console.log("Line ---- "+line);
    });
}


//Process line
var processLines = function (line, db) {
    var res = parseInputLine(line);
    if (res !== undefined) {    ///this is problem
        prepFormat(res, db);
    }
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
            //console.log("result = " + JSON.stringify(result) + " typeof " + typeof result);
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
                                    "samples.sample": OBJ.samples[j].sample,
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
            //db.close()
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
            var SAMPLE = res[j]['sample'];
            var STUDY = res[j]['study_id'];

            // Now create set string for remaining variables
            delete res[j].study_id;
            delete res[j].sample;
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
            setQuery2['sample'] = String(SAMPLE);
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
    if (line.match(/^##/)) {
    }
    else if (line.match(/#CHROM/)) {
        //Get sample index positions
        Header = line.split('\t');
        for (var i = 9; i < Header.length; i++) {
            sampleNames[sampleNames.length] = Header[i];
        }
    }
    else {
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
                formatdata['sample'] = sample_id;
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
            return lineArray;
        }
    } //end else
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