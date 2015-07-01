/*jslint node: true */
/* object class for parsing and retaining variant */

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
    var a = variable.split(',');
    var result = '';
    if (a.length > 1) { result = numberOrStringMulti(a); }
    else { result = numberOrStringSingle(a); }
    return result;
};

var getFormats  = function (strArr) {
    var formatArr = strArr[8].split(':');
    var sampleArr = strArr.slice(9,strArr.length);
    var returnAbleArr = [];
    for (var s=0; s<sampleArr.length; s++) {
        if ( sampleArr[s].match(/\.\/\./) ){
            returnAbleArr[s] = null;
        }
        else{
            var myFormat = {};
            var sampFormat  = sampleArr[s].split(':');
            for (var j=0; j<formatArr.length; j++) {
                myFormat[ formatArr[j] ] = getValue(sampFormat[j]);
            }
            returnAbleArr[s] = myFormat;
        }

    }
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
