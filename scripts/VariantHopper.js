#!/usr/bin/env node
'use strict';


var thisScript = require('commander');
var config = require('./config.json');
var VCFparser = require('./VCF_sampleParse.js');


thisScript
    .version('0.0.1')
    .option('-i, --import', 'Import this VCF')
    .option('-e, --export', 'Export this VCF')
    .option('-v, --input', 'VCF to be processed')
    .option('-n, --name', 'Study Name, for importing')
    .parse(process.argv);

/*
 * Must accept an action!
 */
if(!thisScript.import && !thisScript.export){
    console.log("No action provided. Please include -i or -e.")
    thisScript.outputHelp();
}

/*
 * Setup Database connection(s)
 */
var MongoClient = require('mongodb').MongoClient,
    Server = require('mongodb').Server;
var mongoClient = new MongoClient(new Server(config.db.host, config.db.port));



// Optional Study Name?
var studyName = "defaultStudy_"+Math.random()*1000;
if(thisScript.name){
    studyName = thisScript.name;
}

/*
 * User wants to import variants.
 */
if(thisScript.import){
    console.log("INSIDE THE IMPORT")
    var lr = new LineByLineReader(process.argv[2]);



}





/*
 * require those other scripts...transform to functions only, and call functions from here
 */




/*
 * Set up 1 database connection here...make connection configureable. (could pull from packages or some other .json file.)
 * cascade of processing, calling functions in VCF_export.js & VCF_sampleParse.js
 */



/*
Just do a require('./yourfile.js');

Declare all the variables that you want outside access as global variables. So instead of

var a = "hello" it will be

GLOBAL.a="hello" or just

a = "hello"

This is obviously bad. You don't want to be polluting the global scope. Instead the suggest method is to export your functions/variables.

If you want the MVC pattern take a look at Geddy.
*/
