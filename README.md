# VCFNodeMongo

### Setup/Install

* Node.js - [Download & Install Node.js](http://www.nodejs.org/download/) and the npm package manager.
* MongoDB - [Download & Install MongoDB](http://www.mongodb.org/downloads), and make sure it's running on the default port (27017).

To install Node.js dependencies you're going to use npm, in the application folder run this in the command-line:
```
$ npm install
```


### Current Development

Scripts I have right now:

1. VCF_sampleParse.js
	
	Usage: VCF_import.js **vcf_file** **study_name**

	This script takes a VCF and imports it into the mongodb database
	Known issues:
		Doesn't automatically close when finished

2. VCF_export.js  

	Was working for another project but needs to be modified for this one



Other notes:
	#This is how I inserted the meatdb for now.
	/c/Program\ Files/MongoDB/Server/3.0/bin/mongoimport.exe --db dev --collection meta --type tsv --headerline --file examples/SampleMeta.txt
