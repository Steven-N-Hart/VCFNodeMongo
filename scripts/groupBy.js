'use strict';

var CmdLineOpts = require('commander');
var MongoClient = require('mongodb').MongoClient;
var config = require('./config.json');
var assert = require('assert');
//var _ = require('underscore');
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
	//var collection = db.collection(config.names.sample);
	filterAnnotations(db)
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
/*
Assuming this command:
groupBy.js -g STATUS -a "CAVA_TYPE = Substitution" -a "AC > 0" -f "DP > 0"
I expect the following query



*/



if (typeof excludeSamples === null){
	//Sets a dummy variable so that I am always excluding something
	// It won't work if you don't and I havent figured out why yet
	excludeSamples=1
}

var filterAnnotations = function(db){
	var q = []
	var filters = CmdLineOpts.annotationFilters
	if (filters.length === 0){
		//go to next step
		console.log('no filter applied, moving to next step')
		q = q +'{unwind: "$samples"}'
		filterSamples(q, db)
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
		// q is for annotation filters
		q = filterQuery.join(',')
		//console.log(q)
		filterSamples(q, db)
		//process.exit()
	}
}

var filterSamples = function(q, db){
var filters = CmdLineOpts.filterSampleInfo
//p is sample level filters
var p =''
	if (filters === 0){
		//console.log('No sample level filters applied')
		//process.exit()
		getSamples(p, db)
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
		 getSamples(q, p, db)
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


var getSamples = function(q, p, db){
  var collection = db.collection(config.names.sample);
  /* Need to get the sampleIDs (excluding any if specified) grouped by the aggregation key
  In this case, the feature that I want to group by is the STATUS attribute in the meta field
   The command should look like this if there aren't any sample exclusions:

   db.meta.aggregate(
   [
     { $match : { STATUS: { $exists: true}  }}, 
     { $group : { _id : "$STATUS", samples: { $push: "$_id" } } }
   ]
	)
		Otherwise, if there are samples to be excluded, the query should look like this:

	db.meta.aggregate(
   [
     { $match : { qty: { $exists: true, $nin: [ 'SAMPLE LIST'] } } } ,
     { $group : { _id : "$STATUS", samples: { $push: "$_id" } } }
   ]
	)
	*/
			//build a JSON instead of the string

  var aggregationQuery = {}
  var featureObj = {}
  var existsObj = {}
	var matchObj = {}
	var samplesObj = {}
	var idObj = {}
	var groupObj = {}
	existsObj['$exists'] = true
	if(CmdLineOpts.excludeSamples){
		var excludedSamples = CmdLineOpts.excludeSamples.split(/,/)
		existsObj['$nin'] = excludedSamples
	} 
	matchObj[CmdLineOpts.groupBy] = existsObj
	featureObj['$match'] = matchObj
	//console.log(JSON.stringify(featureObj))

	//clear out the samples obj
	samplesObj = {}
	samplesObj['$push']= "$_id"

	groupObj['_id']="$"+CmdLineOpts.groupBy
	groupObj['samples'] = samplesObj
	var group2 = {}
	group2['$group'] = groupObj
  //process.exit()
  var sampleObj = collection.aggregate([featureObj, group2])
	
	// Get all the aggregation results
	sampleObj.toArray(function(err, docs) {
		console.log('sampleObject = ',docs)
	  console.log('sample-level filters = ', p)
 		console.log('annotation-level filters = ', q)
 		var fieldsToProject = buildAnnotationsForExport()
 		console.log('fieldsToProject = ',fieldsToProject)
 		console.log('Died at line 230.  Need to figure out how to do the join and what the full query should look like')
  	process.exit()
	})

}


var buildAnnotationsForExport = function(){
	var string = '{_id:0, chrom:1, pos:1, ref:1, alt:1 '
	var array = CmdLineOpts.annotationExport
		if(array.length > -1){
			for (var i=0;i< array.length; i ++){
				if(i==0){string = string+', annotation.'+array[i] + ':1'}
					else{string = string+', annotation.'+array[i] + ':1'}
			}
		}
		string = string+'}'
	return string
}
