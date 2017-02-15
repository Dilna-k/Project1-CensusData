data = LOAD '/niit/census/sample.dat' USING PigStorage(',') AS (Age,Education:chararray,maritalstatus:chararray,gender:chararray,TaxFilerStatus:chararray,Income:chararray,Parents:chararray,CountryOfBirth:chararray,Citizenship:chararray,WeeksWorked:chararray);
generategender = FOREACH data GENERATE $3;
groupbygender= GROUP generategender BY gender;
count= FOREACH groupbygender GENERATE group AS gender,COUNT($1);
dump count;
