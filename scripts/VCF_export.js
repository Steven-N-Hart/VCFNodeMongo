/*jslint node: true */
/*jshint -W069 */
'use strict';

// Connection URL
var url = 'mongodb://localhost:27017/dev';
//async = require("async");

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
var  studyIDs=studyID.split(',');

//console.log('studyID='+ studyIDs.length+' '+studyIDs)
//process.exit();

//Step0.  Print header

//Step1.  Make sure the study ID exists in db
getHeader(studyIDs);
//Step2.  Get the sample IDs from DB
//Step3.  Print out the header
//Step4.  Get the variants
//Step5.  Print each variant row


///////////////////////////////////////
// Build functions
function getHeader(studyIDs){
    printHeader();
    getSamples(studyIDs)
  };


// Use connect method to connect to the Server to get the sample ids i need to look for
function getSamples(studyIDs,cb){
  var SAMPLES = [];

  MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    //console.log('Connected')
    var collection = db.collection('meta');
       collection.find({'studyID' : {$in: studyIDs}},{'_id':0,'sample_id':1}).toArray(
            function(err, documents) {
              if (err){console.log('ERR='+err)}
                //console.log('docs='+JSON.stringify(documents));
              for (var j=0; j<documents.length;j++){
              //console.log('j='+documents[j].sample_id);
              SAMPLES.push(documents[j].sample_id);
            }
            //console.log('ARRAY='+SAMPLES);
            //db.close();
            printHeaderLine(db,SAMPLES,printVariants(SAMPLES,db))
            }
        );
  });
}

var COUNT=0;


function printHeaderLine(db,SAMPLES,callback) {
	var HEADER_LINE='#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT';
	for (var i=0;i<SAMPLES.length;i++){
		HEADER_LINE=HEADER_LINE+'\t'+SAMPLES[i];
		}
	console.log(HEADER_LINE);
	stream.write(HEADER_LINE+'\n');
	printVariants(SAMPLES,db);
  	};


function printVariants(SAMPLES,db){
  //console.log('COUNT='+COUNT);COUNT=COUNT+1;
	var collection = db.collection('variants');
	//console.log('studies = '+studies);
  	var cursor = collection.aggregate([
      {$match : {
        'samples.sample':{
            $in:SAMPLES
          }
      }},
      {
          $unwind:"$samples"
      },
      {
        $match:{
          'samples.sample':{
              $in:SAMPLES
          }
        }
      },
      {
        $group:{
            _id:{chr:'$chr',pos:'$pos',ref:'$ref',alt:'$alt'},samples:{$push:'$samples'}
        }
      }
      ]);
  	cursor.each(function(err,res){
      if(err){console.log('err = ' + err)}
		  var variants = res;
  		if(res !== null){
     		//variants= JSON.parse(variants);
     		var chrom = variants._id.chr;
     		var pos = variants._id.pos;
     		var ref = variants._id.ref;
     		var alt = variants._id.alt;
        var ARRAY = variants.samples;
     		var FORMAT_FIELDS = {};
     		var studyAN=0;
     		var studyAC=0;
     		//Get number of samples in study
     		var len = SAMPLES.length;

     		//loop over each sample and get results
     		for (var whichSample=0;whichSample < len ;whichSample++){
     			var SAMPLE = SAMPLES[whichSample];
          //console.log('ARRAY='+JSON.stringify(ARRAY))

          var a = ARRAY.map(function(e) { return e.sample }).indexOf(SAMPLE);
     			//if a < 0, then the sample does not have data
     			if (a < 0){
     				//console.log('This is not in query');
            var sampleObj = new sampleData(SAMPLE,'.','.', '.', '.', '.','.')
     				//break
     			}
          else{
            var sampleObj = new sampleData(ARRAY[a].sample,ARRAY[a].GT, ARRAY[a].GQ, ARRAY[a].HQ, ARRAY[a].PL, ARRAY[a].AD, ARRAY[a].DP)
          }
     	  //console.log('OBJ='+JSON.stringify(sampleObj))
        //process.exit();
      		//Calcuacate AC and AN
     			if (sampleObj.GT === undefined || sampleObj.GT == './.'|| sampleObj.GT == '.' ){
  	   			sampleObj.GT='.';
  	   			studyAN++;studyAN++;
  	   		}
  	   	   	//Add 1 for heterozygous
  	   		if (sampleObj.GT == '0/1' || sampleObj.GT == '0|1'||sampleObj.GT == '1/0' || sampleObj.GT == '1|0' ||sampleObj.GT == './1' ){
  	   			studyAC++;studyAN++;studyAN++;
  	   			//console.log('Added 1')
  	   		}
  	   		//Add 2 for homozygous
  	   		if (sampleObj.GT == '1/1' || sampleObj.GT == '1|1'  ){
  	   			studyAC++;
  	   			studyAC++;studyAN++;studyAN++;
  	   		}
  	   		var formatData = sampleObj.GT+':'+sampleObj.AD+':'+sampleObj.DP+':'+sampleObj.GQ+':'+sampleObj.HQ;
          //console.log(JSON.stringify(formatData))
  	   		FORMAT_FIELDS[sampleObj.sample] = formatData ;
        //console.log('JSON='+JSON.stringify(FORMAT_FIELDS))
  	   	}//All samples have been evaluated

        //console.log(JSON.stringify(FORMAT_FIELDS))
     		var studyAF=0;
     		if (studyAC > 0 ){studyAF=studyAC/studyAN;}
     		var INFO_LINE='AC='+studyAC+';AN='+studyAN+';AF='+studyAF+';';
     		var FULL_LINE = [];
        var GTarray=[];
        for(var o in FORMAT_FIELDS) {
            GTarray.push(FORMAT_FIELDS[o]);
          }
     		FORMAT_FIELDS=GTarray.join('\t');
        //console.log('FORMAT_FIELDS='+FORMAT_FIELDS)
        //console.log('INFO_LINE='+INFO_LINE)

     		FULL_LINE.push(chrom,pos,'.', ref,alt,'.','.',INFO_LINE,'GT:AD:DP:GQ:HQ',FORMAT_FIELDS);
     		var fullLine = FULL_LINE.join('\t');
     		stream.write(fullLine+'\n');
        console.log(fullLine)
     	}//End if res is not null
      else {
        process.exit()
      }
   }//End Each
  );
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



 function sampleData(sample, GT, GQ, HQ, PL, AD, DP) {
    this.GT = isDefined(GT);
    this.GQ = isDefined(GQ);
    this.HQ = isDefined(HQ);
    this.PL = isDefined(PL);
    this.AD = isDefined(AD);
    this.DP = isDefined(DP);
    this.sample = sample;
}

function isDefined(query){
  if (query === undefined){
    return '.'
  }
  else{
    return query
  }
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