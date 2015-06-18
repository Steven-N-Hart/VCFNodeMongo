#!/usr/bin/env node

var thisScript = require('commander');
/*
 * require those other scripts...transform to functions only, and call functions from here
 */

thisScript
    .version('0.0.1')
    .option('-i, --import', 'Import this VCF')
    .option('-e, --export', 'Export this VCF')
    .option('-v, --input', 'VCF to be processed')
    .option('-n, --name', 'Study Name, for importing')
    .parse(process.argv);


if(!thisScript.import && !thisScript.export){
    console.log("No action provided. Please include -i or -e.")
    thisScript.outputHelp();
}



/*
 * Set up 1 database connection here...make connection configureable. (could pull from packages or some other .json file.)
 * cascade of processing, calling functions in VCF_export.js & VCF_sampleParse.js
 */