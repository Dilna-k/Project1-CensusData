data = LOAD '/niit/censusdata/censusdata' USING PigStorage(',') AS (Age,Education:chararray,maritalstatus:chararray,gender:chararray,TaxFilerStatu$
generategender = FOREACH data GENERATE $3;
groupbygender= GROUP generategender BY gender;
count= FOREACH groupbygender GENERATE group AS gender,COUNT($1);
dump count;
