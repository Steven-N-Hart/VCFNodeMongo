/*jslint node: true */
/*jshint -W069 */
'use strict';

var MongoClient = require('mongodb').MongoClient , assert = require('assert');
var mongoServer = require('mongodb').Server;
var serverOptions = {
    'auto_reconnect': true,
    'poolSize': 5
};

var mod_getopt = require('posix-getopt');
var parser, option, output, studyID;

parser = new mod_getopt.BasicParser(':S:(studyID)o:(output)', process.argv);

while ((option = parser.getopt()) !== undefined) {
    switch (option.option) {
    case 'S':
        studyID = option.optarg;        
        break;
    case 'o':
        output = option.optarg;
        break;
    default:
        /* error message already emitted by getopt */
        mod_getopt.equal('?', option.option);
        break;
    }
}

if (output == null || studyID == null){
    console.log('missing required argument: "output"');
    console.log('\nUsage: node VCF_Export [options] <outname.vcf> ');
    console.log('\t-S, studyID [multiple are comma seperated]');
    console.log('\nBy default, all samples in all studies are output\n');
    process.exit();
}

//open the outfile
var fs = require('fs');
var stream = fs.createWriteStream(output);


// Split each study id and sample id and put quotes around them
var studyIDs = [];
studyIDs = studyID.split(',');
studyIDs=studyIDs.join(',');

//Create sample array
var sampleIDs =[];

//Create basic object to use when outputing Samples in VCF
var samplesObj = [];


// Connection URL

var url = 'mongodb://localhost:27017/main';
MongoClient.connect(url, function(err, db) {
	assert.equal(null, err);
  	console.log('Connected correctly to server');
  	printHeader();
  	//stream.once('open', function(fd) {
		getStudyIDs(db,testGlobal);
	});
//});

///////////////////////////////////////
// Build functions
var getStudyIDs = function(db,callback) {
  // Get the documents collection
  var collection = db.collection('variants');
  //Get all the possible studIDs from DB and put into an array
	collection.distinct('studies.study_id', function(err, result) {
		console.log('Availalble Studies ' + result);
		collection.distinct('studies.samples.sample',function(err, SAMPLES) {
			if (err){console.log('Cannot get the sample names from db');}
			sampleIDs = SAMPLES;
			var HEADER_LINE='#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT';
			for (var i=0;i<SAMPLES.length;i++){
				HEADER_LINE=HEADER_LINE+'\t'+SAMPLES[i];
			}

			stream.write(HEADER_LINE+'\n');
		});
    	callback(db,result);
  	});  
};

function testGlobal(db,data){
	//find out if my studyID/sample_ID matches what is in my array
	findStringInArray(studyIDs,data,printStudies,db);
}

function findStringInArray(str, res, cb, db) {
	var Strings = str.split(',');
	var arr = res.toString();
	var Arr = arr.split(',');
	for (var j = 0; j < Strings.length; j++){
		var matchCount = 0;
	    for (var i = 0; i < Arr.length; i++) {
	        if (Arr[i] === Strings[j]) {
	        	matchCount++;
	        }
	    }
	    if(matchCount > 0){
	    	console.log('Study found = '+ Strings[j]);
	    }
	    else{
	    	console.log('Study not found = ' + Strings[j]);  
	    	throw 'Study is not in the list';  	
	    }
	}
	cb(Strings,db);
}

function printStudies(studies,db){
	var collection = db.collection('variants');
	studyIDs = studyID.split(',');
	//studyIDs=studyIDs = '\'' + studyIDs.join('\',\'') + '\'';
  	var cursor = collection.find({'studies.study_id': {$in :  studies }});
  	 cursor.each(function(err,res){
   		if(err){console.log('err = ' + err);}
   		//console.log('res = '+JSON.stringify(res));
		var variants = res;
		if(res !== null){
   		//variants= JSON.parse(variants);
   		var chrom = variants._id.chr;
   		var pos = variants._id.pos;
   		var ref = variants._id.ref;
   		var alt = variants._id.alt;
   		var FORMAT_FIELDS = [];
   		var studyAN=0;
   		var studyAC=0;
   		var len = variants.studies.length;
   		console.log('len = '+ len);
   		for (var whichStudy=0;whichStudy < len ;whichStudy++){
   		//console.log('variants.studies = '+ variants.studies);
   		//console.log('variants.studies = '+ variants.studies.length);
   		var STUDIES = variants.studies[whichStudy];
   		//Number of samples with mutation 
   		//console.log('STUDIES. = '+JSON.stringify(variants.studies));
   		for (var j=0; j < STUDIES.samples.length; j++){
	   			if (STUDIES.samples[j].GT === undefined || STUDIES.samples[j].GT == './.'|| STUDIES.samples[j].GT == '.' ){
	   				STUDIES.samples[j].GT='.';
	   				//console.log('No data for '+JSON.stringify(STUDIES.samples[j]));
	   			}
	   			else{
	   				studyAN++;studyAN++;
	   			}
	   			//Add 1 for heterozygous
	   			if (STUDIES.samples[j].GT == '0/1' || STUDIES.samples[j].GT == '0|1'||STUDIES.samples[j].GT == '1/0' || STUDIES.samples[j].GT == '1|0' ){
	   				studyAC++;
	   				//console.log('Added 1')
	   			}
	   			//Add 2 for homozygous
	   			if (STUDIES.samples[j].GT == '1/1' || STUDIES.samples[j].GT == '1|1' == '1/1' || STUDIES.samples[j].GT == '1|1' ){
	   				studyAC++;
	   				studyAC++;
	   				//console.log('Added 2')
	   			}
	   			if (STUDIES.samples[j].AD === undefined){STUDIES.samples[j].AD='.';}
	   			if(	Array.isArray(STUDIES.samples[j].AD)){ STUDIES.samples[j].AD = STUDIES.samples[j].AD.join();}
	   			if (STUDIES.samples[j].DP === undefined){STUDIES.samples[j].DP='.';}
	   			if (STUDIES.samples[j].GQ === undefined){STUDIES.samples[j].GQ='.';}
	   			if (STUDIES.samples[j].HQ === null){STUDIES.samples[j].HQ='.';}
	   				if (STUDIES.samples[j].HQ == ','||STUDIES.samples[j].HQ ===undefined){STUDIES.samples[j].HQ = '.';}
	   			var formatData = STUDIES.samples[j].GT+':'+STUDIES.samples[j].AD+':'+STUDIES.samples[j].DP+':'+STUDIES.samples[j].GQ+':'+STUDIES.samples[j].HQ;
	   			FORMAT_FIELDS.push(formatData);
	   		}
	   	}
	   	console.log(FORMAT_FIELDS)

   		//console.log('studyAC = '+ studyAC + ' StudyAN = ' + studyAN);
   		var studyAF=0;
   		if (studyAC > 0 ){studyAF=studyAC/studyAN;}
   		var INFO_LINE='AC='+studyAC+';AN='+studyAN+';AF='+studyAF+';';
   		var FULL_LINE = [];

   		FORMAT_FIELDS=FORMAT_FIELDS.join('\t');
   		FULL_LINE.push(chrom,pos,'.', ref,alt,'.','.',INFO_LINE,'GT:AD:DP:GQ:HQ',FORMAT_FIELDS);
   		var fullLine = FULL_LINE.join('\t');
   		stream.write(fullLine+'\n');
   	}
   	else{
   		stream.end();
   		db.close();
   	}
   	});
}

function printHeader(){
	stream.write('##fileformat=VCFv4.2\n');
	stream.write('##INFO=<ID=AF,Number=1,Type=Float,Description="Allele Frequency">\n');
	stream.write('##INFO=<ID=AC,Number=1,Type=Float,Description="Number of alleles with variant">\n');
	stream.write('##INFO=<ID=AN,Number=1,Type=Float,Description="Number of alleles with data">\n');
	stream.write('##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">\n');
	stream.write('##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">\n');
	stream.write('##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">\n');
	stream.write('##FORMAT=<ID=AD,Number=2,Type=Integer,Description="Number of reference and supporting alleles">\n');
	stream.write('##FORMAT=<ID=HQ,Number=2,Type=Integer,Description="Haplotype Quality">\n');
}



 

