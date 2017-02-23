data = LOAD '/niit/censusdata/censusdata' USING PigStorage(',') AS (Age,Education:chararray,maritalstatus:chararray,gender:chararray,TaxFilerStatus:chararray,Income:chararray,Parents:chararray,CountryOfBirth:chararray,Citizenship:chararray,WeeksWorked:chararray);
a = FOREACH data GENERATE $1,$3;
b = GROUP a BY (Education,gender);
c= FOREACH b GENERATE group AS education,COUNT($1);
dump c;
