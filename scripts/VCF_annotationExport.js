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
    .usage('[options] -o outname')
    .option('-o, --output [text]', 'Name of output file')
    .parse(process.argv);

var ts1 = process.hrtime();


//Make sure are variables are set
if (!CmdLineOpts.output) {
    console.log('\nMissing output VCF file name');
    CmdLineOpts.outputHelp();
    process.exit(27); // Exit Code 27: IC68342 = Missing Input Parameters
}


//open the outfile
var fs = require('fs');
var stream = fs.createWriteStream(CmdLineOpts.output);



MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    printHeader()
    printHeaderLine()
    printVariants(db);
  });


function printHeaderLine() {
    var HEADER_LINE='#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE';
    console.log(HEADER_LINE);
    stream.write(HEADER_LINE+'\n');
    };


function printVariants(db){
    var collection = db.collection('variants');

    collection.find({ 'needsAnnotation':true}).each(function(err,res){
      if(err || res == null){ console.log('err='+err)}
          var variants = res;
        //console.log('EACH results= '+JSON.stringify(variants));
        if(res != null){
            var chrom = variants.chr;
            var pos = variants.pos;
            var ref = variants.ref;
            var alt = variants.alt;

            var FULL_LINE = [];
            FULL_LINE.push(chrom,pos,'.', ref,alt,'.','.','.','GT','0/1');
            var fullLine = FULL_LINE.join('\t');
            stream.write(fullLine+'\n');
            console.log(fullLine)
        }//End if res is not null
         
 }//End Each

  );
setTimeout(function(){db.close()}, 10);// I really don't think this is correct
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

