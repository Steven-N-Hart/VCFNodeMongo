/*jslint node: true */
/* object class for parsing and retaining variant */

module.exports = {
    parseVCFline : function(vcfLine){
        var row = vcfLine.split('\t');
        if (row[4].match(',')) {
            console.error(vcfLine);
            throw ('I cannot handle multiple alleles!!!');
        }
        var variant = {
            chr : row[0],
            pos : Number(row[1]),
            ref : row[3],
            alt : row[4]
        };
        return variant;
    }
};





//var FORMAT = Row[8].split(':');
