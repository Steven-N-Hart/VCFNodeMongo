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
    .usage('[options] -n studyname -o outname')
    .option('-o, --output [text]', 'Name of output file')
    .option('-n, --studyname [text]', 'Study Name, for importing')
    .parse(process.argv);

var ts1 = process.hrtime();


//Make sure are variables are set
if (!CmdLineOpts.output) {
    console.log('\nMissing output VCF file name');
    console.log('\tmultiple studynames are comma seperated');
    console.log('\nBy default, all samples in all studies are output\n');
    CmdLineOpts.outputHelp();
    process.exit(27); // Exit Code 27: IC68342 = Missing Input Parameters
}

if (!CmdLineOpts.studyname) {
    console.log("\nMissing Study Name.");

    CmdLineOpts.outputHelp();
    process.exit(27);
}
//open the outfile
var fs = require('fs');
var stream = fs.createWriteStream(CmdLineOpts.output);


// Split each study id and sample id and put quotes around them
var studyID = CmdLineOpts.studyname;
var studyIDs=studyID.split(',');

//console.log('studyID='+ studyIDs.length)
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
    getSamples(studyIDs,printHeaderLine(db,SAMPLES,printVariants(SAMPLES,db))
)
  };


// Use connect method to connect to the Server to get the sample ids i need to look for
function getSamples(studyIDs,cb){
  var SAMPLES = [];

  MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    //console.log('Connected')
    var collection = db.collection('meta');
       collection.find({'study_id' : {$in: studyIDs}},{'_id':0,'sample_id':1}).toArray(
            function(err, documents) {
              if (err){console.log('ERR='+err)}
               // console.log('docs='+JSON.stringify(documents));
              for (var j=0; j<documents.length;j++){
              //console.log('j='+documents[j].sample_id);
              SAMPLES.push(documents[j].sample_id);
            }
            //console.log('ARRAY='+SAMPLES);
            //db.close();
            cb()
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
 //console.log('studyIDs = '+studyIDs);
    //console.log('samples = '+SAMPLES);
  	var cursor = collection.aggregate([
      {$match : {
        'samples.sample_id':{
            $in:SAMPLES
          }
      }},
      {
          $unwind:"$samples"
      },
      {
        $match:{
          'samples.sample_id':{
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
    console.log('Starting EACH');
  	cursor.each(function(err,res){
      if(err || res == null){console.log('err = ' + err);}
		  var variants = res;
      //console.log('EACH results= '+JSON.stringify(variants));
  		if(res != null){
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
        //console.log('ARRAY='+JSON.stringify(ARRAY))

     		//loop over each sample and get results
     		for (var whichSample=0;whichSample < len ;whichSample++){
     			var SAMPLE = SAMPLES[whichSample];

          var a = ARRAY.map(function(e) { return e.sample_id }).indexOf(SAMPLE);
          //console.log('Looking for '+SAMPLE+' in '+ARRAY);
          //console.log('found: '+a);

     			//if a < 0, then the sample does not have data
     			if (a < 0){
            var sampleObj = new sampleData(SAMPLE,'.','.', '.', '.', '.','.','.')
     				//break
     			}
          else{
            var sampleObj = new sampleData(ARRAY[a].sample_id,ARRAY[a].GT, ARRAY[a].GQ, ARRAY[a].HQ, ARRAY[a].PL, ARRAY[a].AD, ARRAY[a].DP, ARRAY[a].GTC)
          }
     	  //console.log('OBJ='+JSON.stringify(sampleObj))
        //process.exit();
      		//Calcuacate AC and AN
     			if (sampleObj.GT === undefined || sampleObj.GT == './.'|| sampleObj.GT == '.' ){
  	   			sampleObj.GT='.';
  	   			
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
  	   		var formatData = sampleObj.GT+':'+sampleObj.AD+':'+sampleObj.DP+':'+sampleObj.GQ+':'+sampleObj.HQ+':'+sampleObj.GTC;
          //console.log(JSON.stringify(formatData))
  	   		FORMAT_FIELDS[sampleObj.sample_id] = formatData ;
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

     		FULL_LINE.push(chrom,pos,'.', ref,alt,'.','.',INFO_LINE,'GT:AD:DP:GQ:HQ:GTC',FORMAT_FIELDS);
     		var fullLine = FULL_LINE.join('\t');
     		stream.write(fullLine+'\n');
        //console.log(fullLine)
     	}//End if res is not null
      else {
        setTimeout(function(){db.close();}, 10); // I really don't think this is correct
      }
     
 }//End Each
  );
       setTimeout(function(){db.close()}, 1);// I really don't think this is correct
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
  stream.write('##FORMAT=<ID=GTC,Number=1,Type=Integer,Description="Number of alternate alleles in sample">\n');
}



 function sampleData(sample, GT, GQ, HQ, PL, AD, DP, GTC) {
    this.GT = isDefined(GT);
    this.GQ = isDefined(GQ);
    this.HQ = isDefined(HQ);
    this.PL = isDefined(PL);
    this.AD = isDefined(AD);
    this.DP = isDefined(DP);
    this.GTC = isDefined(GTC);
    this.sample_id = sample;
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