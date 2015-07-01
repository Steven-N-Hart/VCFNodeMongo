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

#### 1. Importing Variants
	
```Usage: VCF_import.js -i [vcf_file] -n [study_name] ```

This script takes a VCF and imports it into the mongodb database

* Known issues:
		Doesn't automatically close when finished

#### 2. Exporting Variants
```Usage: node scripts/VCF_export.js -S [study names] [outname] ```

This script extracts the VCF from the samples in the study name.  It can be either 1 study, or a comma separated list of studies.

* Known issues:
		Seems to be working, but I have a hacky process.exit in there that I don't quite like


#### 3. Importing Samples
```Usage: node scripts/Meta_import.js examples/SampleMeta.txt ```

This script takes a TSV file containing sample information and loads it into the DB.
The TSV requires 3 fields: sample_id, study_id, and kit_id.
Anything else is optional (e.g. case or control, substudy, etc)

#### 4. Exporting variants that need to be annotated
```Usage: node scripts/VCF_annotationExport.js -o outname ```

This script gets all variants from the DB that need annotation 
