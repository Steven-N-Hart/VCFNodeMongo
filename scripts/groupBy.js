'use strict';

var CmdLineOpts = require('commander');
var MongoClient = require('mongodb').MongoClient;
var config = require('./config.json');
var assert = require('assert');
var _ = require('underscore');
/*
 * Setup Commandline options, auto generates help
 */
CmdLineOpts
    .version('0.0.1')
    .usage('[options] ')
    .option('-g, --groupBy [group]', 'Meta field to group results by')
    .option('-x, --excludeSamples [samples]', 'comma seperated list of samples to exclude','na')
    .option('-f, --filterSampleInfo [filters]', '-f "GT = 0/1" -f "DP > 10"',collect, [])
    .option('-a, --annotationFilters [filters]', '-a "AC > 0" -a "CAVA_IMPACT = HIGH|MODERATE"', collect, [])
    .option('-A, --annotationExport [data2export]', '-A AC -A CAVA_IMPACT -A chr -A pos -A ref -A alt', collect, [])
    .parse(process.argv);

var url = 'mongodb://' + config.db.host + ':' + config.db.port + '/' + config.db.name;
MongoClient.connect(url, function (err, db) {
	assert.equal(null, err);
	var collection = db.collection(config.names.sample);
	filterAnnotations(collection,db)
	})

function collect(val, memo) {
  memo.push(val);
  return memo;
}

// Apply Annotation filters
// unwind samples
// Sample-specific filters
// exclude unwanted samples
// aggregate results
// write out file

var filterAnnotations = function(collection, db){
	var q = []
	var filters = CmdLineOpts.annotationFilters
	if (filters.length === 0){
		//go to next step
		console.log('no filter applied, moving to next step')
		q = q +'{unwind: "$samples"}'
		filterSamples(q, collection, db)
		//process.exit()
	} else {
		//deconstruct the query
		var filterQuery = []
		for (var i=0;i<filters.length;i++){
				var array = filters[i].split(/(<=|>=|!=|=|>|<)/,3)
				//remove trailing spaces
				array[0]=array[0].replace(/ $/,'')
				//remove leading spaces
				array[2]=array[2].replace(/^ /,'')
				//console.log(array)
				var operator = defineOperator(array[1])
				var preCheck = checkForOr('annotation',array[0], operator, array[2])
				filterQuery.push(preCheck)
				//process.exit()
		} // end for loop
		q = filterQuery.join(',')
		//console.log(q)
		filterSamples(q, collection, db)
		//process.exit()
	}
}

var filterSamples = function(q, collection, db){
var filters = CmdLineOpts.filterSampleInfo
var p =''
	if (filters === 0){
		//console.log('No sample level filters applied')
		//process.exit()
		excludeSamples(p,collection, db)
	} else {
		//deconstruct the query	
		var filterQuery = []
		for (var i=0;i<filters.length;i++){
				var array = filters[i].split(/(<=|>=|!=|=|>|<)/,3)
				//remove trailing spaces
				array[0]=array[0].replace(/ $/,'')
				//remove leading spaces
				array[2]=array[2].replace(/^ /,'')
				//console.log(array)
				var operator = defineOperator(array[1])
				var preCheck = checkForOr('samples',array[0], operator, array[2])
				filterQuery.push(preCheck)
				//process.exit()
		} // end for loop
		p = filterQuery.join(',')
		 excludeSamples(q,p,collection, db)
		//process.exit()
	}
}


var checkForOr = function(label, key, operator, value){
	var or = value.match(/\|/)
	//console.log('or = ', or[0])
	if (or === null){ 
		// if it is a number, this approach works { qty: { $lt: 20 } }
		// other wise you need "EFFECT: HIGH"
		if (isNaN(Number(value))) {
        value = '"'+label+'.'+key+'":'+'"'+value+'"'
    }
    else {
        value = '"'+label+'.'+key+'":{ '+operator+' : '+value+' }'
    }

		//console.log('value = ',value)
		return value 
	}
	else{
		var array = value.replace(/\|/,'","')
		//array = '{ "'+label+'.'+key+'": { $in: ["'+array+'"] } }' 
		array = '"'+label+'.'+key+'": { $in: ["'+array+'"] }' 

		//console.log('array = ', array)
		return array
	}

}



var defineOperator = function(operator){
	var outSign
	switch(operator) {
	    case '>=':
	        outSign = "$ge"
	        break;
	    case '<=':
	        outSign = "$le"
	        break;
	    case '>':
	        outSign = "$gt"
	        break;
	    case '<':
	        outSign = "$lt"
	        break;
	    case '=':
	        outSign = "$eq"
	        break;
	    case '!=':
	        outSign = "$ne"
	        break;
	}
	return outSign
}



var excludeSamples = function(q, p, collection, db){
	//get samples
	//db.meta.find({STATUS:{$exists:true}},{_id:1})
	var SAMPLES = [];
	var key = CmdLineOpts.groupBy
	var sampleQuery = {}
	var exists =  {}
	exists['$exists'] = true
	sampleQuery[key] = exists
		//console.log(sampleQuery)
	var id={}
	id['_id'] = 1
	var SampleArray=[]
	var excludedSamples =[]
	if(CmdLineOpts.excludeSamples){
	 excludedSamples = CmdLineOpts.excludeSamples.split(',')
	}
	//process.exit()
	collection.find(sampleQuery).toArray(function(err, docs){
		for (var i=0;i<docs.length;i++){
     		SampleArray.push(docs[i]._id)
			}
	//console.log('Excluding samples: ', excludedSamples)
	//console.log('Before: ',SampleArray)
	var samplesToKeep=[]
	//exclude specified samples
	//console.log('SampleArray.length: ',SampleArray.length)
	for (var i=0; i< SampleArray.length; i++){
		var keep = 1
		for (var j=0;j<excludedSamples.length;j++){
			if (SampleArray[i] == excludedSamples[j]){
				keep = 0
			}
		if (keep === 1){
			samplesToKeep.push('"'+SampleArray[i]+'"')
			}
		}
	}
	
	console.log('After: ',samplesToKeep,q)
	var samples ='}},{$unwind: "$samples"},{$match:{"samples.sample_id":{$in:['+samplesToKeep+']},'+p+'}'
	q = q+samples
	q ='{$match:{'+q+'}'
	getGroupKeys(q,db)
	// group results
	//process.exit()
	}
	) // done with adding samples	

}


var getGroupKeys = function(queryString,db){
		//get back what I want to project
		var fieldsToProject = buildAnnotationsForExport(db)
		var collection = db.collection(config.names.sample);
		var key = '"'+CmdLineOpts.groupBy+'"'
		//console.log(collection)
		console.log(queryString,fieldsToProject)
		process.exit()

}


var buildAnnotationsForExport = function(db){
	var string = '{_id:0, '
	var array = CmdLineOpts.annotationExport
		if(array.length > -1){
			for (var i=0;i< array.length; i ++){
				if(i==0){string = string+array[i] + ':1'}
					else{string = string+ ', '+array[i] + ':1'}
			}
		}
		string = string+'}'
	return string
}
