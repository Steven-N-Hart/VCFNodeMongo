###VCF_import
1.  Make sure we don't overwrite positive calls with negative ones if the sample exists.
2.  Only add GTC if it isnt provided

###VCF_export
1.  Validate large file output
2.  GroupBy query
3.  Get Annotation in VCF output (INFO field)

###TSV_annotationImport
1. require chrom, pos, ref, alt
2. All other headers get scrubbed for non-unicode characters
3. add to annotation object
 
###Server
1.  Get Authentication (OAuth/OmniAuth - Google only for now)
2.  Authorization
3.  Admin role in config.json

###APIs
1.
2.

###Possible Names?
1. Poly-Mut-Cache
2. Variant-Arcade
3. VCF-Stash
4. Collated-Var-Index
5. Joint-Genetic-Store
