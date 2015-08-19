/*jslint node: true */
/* object class for parsing and retaining variant */

//Get header
function get_headerLine(filename, callback) {
    var stream = fs.createReadStream(filename, {
      flags: 'r',
      encoding: 'utf-8',
      fd: null,
      mode: 0666,
      bufferSize: 64 * 1024
    });

    var fileData = '';
    stream.on('data', function(data){
      fileData += data;

      // The next lines should be improved
      var lines = fileData.split("\n");

      if(lines.match(/^#CHROM/) ){
        stream.destroy();
        console.log('LINES[0]='+lines[0])
        process.exit()
        callback(null, lines[0]);
      }
    });

    stream.on('error', function(){
      callback('Error', null);
    });

    stream.on('end', function(){
      callback('File end reached without finding line', null);
    });

}

//Get number of alt genotypes
function getGTC(GT) {
    GT = JSON.stringify(GT);
    GT = GT.replace(/[\/\|\.0\"]/gi, '');
    return GT.length;
}

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
function numberOrStringMulti(variable) {
    var result = variable.map(function (x) {
        if (isNaN(Number(x))) {
            if (x !== '.') {
                return String(x);
            }
        }
        else {
            return Number(x);
        }
    });
    return result;
};

var getValue  = function (variable) {
    // for each value, convert to a number if it is one.
    var result = '';
    if (variable === undefined ){
        result = null
    } else {
        if (variable.match(/,/)){
            var a = variable.split(',');
            //console.log('this is an array')
            result = numberOrStringMulti(a)
        } else {
            var a = variable
            result = numberOrStringSingle(a); 
            }
    }
    return result;
};

var getFormats  = function (strArr) {
    var formatArr = strArr[8].split(':');
    //console.log('formatArr'+formatArr)
    var sampleArr = strArr.slice(9,strArr.length);
    var returnAbleArr = [];
    for (var s=0; s<sampleArr.length; s++) {
        //console.log('looking at '+sampleArr[s])
        if ( sampleArr[s].match(/^\.\/\./) || sampleArr[s].match(/^\.$/) ){
            returnAbleArr[s] = null;
        }
        else{
            var myFormat = {};
            var sampFormat  = sampleArr[s].split(':');
            for (var j=0; j<formatArr.length; j++) {
                //only keep these fields: GT:AD:DP:GQ:HQ
                // if the GT is not ./.
                if (formatArr[j].match(/^(GT|AD|DP|GQ|HQ)$/)){
                    //console.log('Format of '+j+'='+formatArr[j]+'\t'+sampFormat[j]+'\tSampleArrs='+sampleArr[s])
                    var res = getValue(sampFormat[j])
                    //console.log('sample value = '+ res +' from '+ formatArr[j] + ' and ' + sampFormat[j])
                    if (res !== '\.'){
                            myFormat[ formatArr[j] ] = res;
                    }
                }
            }
            myFormat['GTC'] = getGTC(myFormat['GT'])
            returnAbleArr[s] = myFormat;
             //console.log('returnAbleArr[s]='+JSON.stringify(returnAbleArr[s]))

        }     
    }
    //console.log('returnAbleArr='+JSON.stringify(returnAbleArr))
    //process.exit()
    return returnAbleArr;
};




module.exports = {
    parseVCFline : function(vcfLine, callback){
        var row = vcfLine.split('\t');
        if (row[4].match(',')) {
            console.error(vcfLine);
            throw ('I cannot handle multiple alleles!!!');
        }
        var wholeRecord = {
            variant : {
                chr : row[0],
                pos : Number(row[1]),
                ref : row[3],
                alt : row[4]
                // will have samples : [{GT:"",sampleObjId:"",HQ:"",...}] where sample is already uploaded...refuse non-existant samples
            },
            //sampleNames : vcfTitle.slice(9,vcfTitle.length),
            sampleFormats : getFormats(row)
        };
        callback( wholeRecord );
    }


};


/*
intenal format - no object where sample format = null (./.)
"samples" : [
    {
        "GT" : "0|1",
        "GQ" : 48,
        "DP" : 8,
        "HQ" : [
            51,
            51
        ],
        "sample_id" : "55945ed9dc2aa87d210c1ee2",
    },
    {
        "GT" : "0/1",
        "GQ" : 43,
        "DP" : 5,
        "HQ" : [
            null,
            null
        ],
        "study_id" : "TestStudy",
        "sample" : "EX3"
    },
*/

