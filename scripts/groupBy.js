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



MongoClient.connect(url, function(err, db) {
	//var test = testDB(db,url);
	
  filterAnnotations(db)
});


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
groupBy.js -g STATUS -a "CAVA_TYPE = Substitution" -a "AC > 0" -f "DP > 0" -A AC -A CAVA_IMPACT
I expect the following query


*/

if (typeof excludeSamples === null){
	//Sets a dummy variable so that I am always excluding something
	// It won't work if you don't and I havent figured out why yet
	excludeSamples=1
}

var filterAnnotations = function(db){
	var q = {}
	var filters = CmdLineOpts.annotationFilters
	//console.log('filters.length = ',filters.length, 'CmdLineOpts.annotationFilters: ', CmdLineOpts.annotationFilters)
	if (filters.length === 0){
		//go to next step
		console.log('no annotation filter applied, moving to next step')
		//q['unwind'] = "$samples";
		filterSamples(q, db)
		//process.exit()
	} else {
		//deconstruct the query
		var filterQuery = {}
		for (var i=0;i<filters.length;i++){
				var array = filters[i].split(/(<=|>=|!=|=|>|<)/,3)
				//remove trailing spaces
				array[0]=array[0].replace(/ $/,'')
				//remove leading spaces
				array[2]=array[2].replace(/^ /,'')
				//console.log(array)
				var operator = defineOperator(array[1])
				var preCheck = checkForOr('annotation',array[0], operator, array[2],filterQuery)
				//console.log('precheck = ', preCheck)
				//process.exit()
		} // end for loop
		// q is for annotation filters
		//console.log('Annotation filter query = ',filterQuery)
		//process.exit()
		q = filterQuery
		filterSamples(q, db)
	}
}

var filterSamples = function(q, db){
var filters = CmdLineOpts.filterSampleInfo
//p is sample level filters
var p = {}
	if (filters.length === 0){
		console.log('No sample level filters applied')
		//process.exit()
		getSamples(q, p, db)
	} else {
		//deconstruct the query	
		//console.log('This assumes sample filter is set ', filters)
		var filterQuery = {}
		for (var i=0;i<filters.length;i++){
				var array = filters[i].split(/(<=|>=|!=|=|>|<)/,3)
				//remove trailing spaces
				array[0]=array[0].replace(/ $/,'')
				//remove leading spaces
				array[2]=array[2].replace(/^ /,'')
				//console.log(array)
				var operator = defineOperator(array[1])
				var preCheck = checkForOr('samples',array[0], operator, array[2],filterQuery)
				//console.log('preCheck = ', preCheck)
				//filterQuery.push(preCheck)
				//process.exit()
		} // end for loop
		//console.log('Sample filterQuery = ',filterQuery)
		//process.exit()
		//p = filterQuery.join(',')
		p=filterQuery
		getSamples(q, p, db)
		
	}
}

var testDB = function(db,url){
	console.log('Testing db')


						  // Create a collection we want to drop later
						  var collection = db.collection('simple_query');
console.log('Inserting')
						  // Insert a bunch of documents for the testing
						  collection.insertMany([{a:1}, {a:2}, {a:3}], {w:1}, function(err, result) {
						  });
console.log('Completed Inserting')
						    // Peform a simple find and return all the documents
						collection.find().toArray(function(err, docs) {
							console.log('err : ', err)
							console.log('docs : ', docs)
							process.exit()
    				});
console.log('Completed Finding')
	return 1
	//process.exit()
}

var checkForOr = function(label, key, operator, value, obj){
	var or = value.match(/\|/)
	//console.log('or = ', or[0])
	if (or === null){ 
		// if it is a number, this approach works { qty: { $lt: 20 } }
		// other wise you need "EFFECT: HIGH"
		//console.log('evaluating key value pair ', key, value)
		
		var val = {}
		if (isNaN(Number(value))) {
        //value = '"'+label+'.'+key+'":'+'"'+value+'"
        obj[label+'.'+key]=value
        //console.log('This is not a number ',JSON.stringify(obj))
    }
    else {

        //value = '"'+label+'.'+key+'":{ '+operator+' : '+numberOrStringSingle(value)+' }'
        var valueObj = {}
        var queryObj = {}
        queryObj[operator] = numberOrStringSingle(value)
        obj[label+'.'+key] = queryObj
        //console.log('value is ', JSON.stringify(valueObj))
        return obj
        //process.exit()

    }
		return obj
	}
	else{
		//console.log('evaluating ', value)
		var val ={}
		//console.log('vaue - ', value)
		var array = []
		array = value.split(/\|/)
		//console.log('array filters = ', typeof(array),array)
		var tmpObj = {}
		tmpObj['$in'] = array
		obj[label+'.'+key]=tmpObj
		return obj
	}

}



var defineOperator = function(operator){
	var outSign
	switch(operator) {
	    case '>=':
	        outSign = '$ge'
	        break;
	    case '<=':
	        outSign = '$le'
	        break;
	    case '>':
	        outSign = '$gt'
	        break;
	    case '<':
	        outSign = '$lt'
	        break;
	    case '=':
	        outSign = '$eq'
	        break;
	    case '!=':
	        outSign = '$ne'
	        break;
	}
	return outSign
}


var getSamples = function(q, p, db){
  var collection = db.collection(config.names.sample);
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
  //console.log('featureObj',JSON.stringify(featureObj))
  //console.log('group2',JSON.stringify(group2))
	//process.exit()
  var sampleObj = collection.aggregate([featureObj, group2])
	getVariantCounts(sampleObj,p, q, db);

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


var getVariantCounts = function(sampleObj, p, q, db){
	// Get all the aggregation results
	var collection = db.collection(config.names.variant);	
	//The sampleObj contains the results of the aggregation query [which is just the groups of samples]
	var countsArray = []
	sampleObj.toArray(function(err, groups) {
		//console.log(groups) //groups is an array of group objects that has the group name and the samples that group contains
		// for each group, the name is the _id (e.g. case or control), then there is an array of sample names
		//First, do a query on the variant
		//console.log('here is q',JSON.stringify(q))
		//console.log('GROUPS: ',groups)
		
		collection.find(q).toArray(function(err2, variants){
			//console.log('vars = ', variants)
			for (var groupNum=0; groupNum<groups.length; groupNum++){
				// We need to find out which samples in each group have the variant in question
				var counts = {}
				//console.log('samples in group = ',groups[groupNum])
				for (var sampleNum=0; sampleNum<variants[0].samples.length; sampleNum++){
					var sampleName = variants[0].samples[sampleNum].sample_id
					var GTC = variants[0].samples[sampleNum].GTC
					var groupNames = []
					var groupNames = groups[groupNum].samples
					// See if this sample is in my group or not
					var genotypeCounts = []
					var samplesInGroup = []
					counts['_id'] = groups[groupNum]._id

					for (var s=0; s<groupNames.length; s++){
						if (groupNames[s] == sampleName){
							//console.log('there is a match')
							genotypeCounts.push(GTC)
							samplesInGroup.push(sampleName)
							var gc = genotypeCounts.reduce(function(a, b) { return a + b; }, 0) /  genotypeCounts.length;
							counts['GTC'] = gc
							counts['samples'] = samplesInGroup
						}
					}
				countsArray.push(counts)
				//console.log(counts)
				}	
			//countsArray.push(counts)
			// Counts now contains samples in each group
			}// end the group count
		})// end the array of the find query
	})// end the sampleObj2Array which just defines the sample groups
 	//This is where I need to get variant-level data
 console.log(countsArray)

 };
	//console.log('sample-level filters = ', JSON.stringify(p))
	//console.log('annotation-level filters = ', JSON.stringify(q))
	//var fieldsToProject = buildAnnotationsForExport()
 	//console.log('fieldsToProject = ',fieldsToProject)


var numberOrStringSingle = function (x) {
    var result = '';
    if (isNaN(Number(x))) {
        if (x !== '.') { result = String(x); }
    }
    else {
        result = Number(x);
    }
    return result;
};

