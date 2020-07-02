CREATE TABLE users(
	row_id serial not NULL,
	UserID VARCHAR NOT NULL,
	FirstTxnCompleteTime Timestamp NOT NULL
);

CREATE TABLE Transactions(
	rowID	serial not null,
CountryCode	integer not null,
ChannelID	integer not null, 
ChannelTypeId	integer not null,
UserId	varchar not null, 
MerchantGroupId	integer not null,
TxnId	text not null,
TxnCompleteTime	timestamp not null,
Price	decimal);

CREATE TABLE fx_rates(
	row_id serial not NULL,
	Timing	timestamp not null,
	CountryCode	integer not null,
	Price	decimal);

ALTER TABLE transactions 
ADD COLUMN monthoftxn integer,
ADD COLUMN yroftx integer;

ALTER TABLE fx_rates 
ADD COLUMN monthoftxn integer,
ADD COLUMN yroftx integer;

update transactions set monthoftxn = extract( month from transactions.txncompletetime)
update transactions set yroftx = extract( year from transactions.txncompletetime)

update fx_rates set monthoftxn = extract( month from fx_rates.timing)
update fx_rates set yroftx = extract( year from fx_rates.timing)

ALTER TABLE transactions ALTER COLUMN key1 TYPE VARCHAR;
ALTER TABLE fx_rates ALTER COLUMN key1 TYPE VARCHAR;

select CONCAT(countrycode,monthoftxn, yroftx) 
from transactions t 

update transactions set key1 = (select CONCAT(transactions.countrycode, transactions.monthoftxn, transactions.yroftx))
update fx_rates set key1 = (select CONCAT(fx_rates.countrycode, fx_rates.monthoftxn, fx_rates.yroftx))

ALTER TABLE transactions 
ADD COLUMN fx_rate decimal,
ADD COLUMN final_value decimal;

ALTER TABLE transactions 
RENAME COLUMN final_value TO final_value_USD;

UPDATE transactions 
SET    fx_rate = fx_rates.price 
FROM  fx_rates 
WHERE transactions.key1 = fx_rates.key1

update transactions set final_value = transactions.price/transactions.fx_rate

/*Question 1*/
select date (transactions.txncompletetime), count(distinct userid) as usercount, count(txnid) as txncount, sum(final_value_USD) as totaltxnusd, sum(price) as totaltxnlocal, avg(final_value_USD) as avgtxnusd, sum(final_value_USD)/count(distinct userid) as avgspenduser 
from transactions
where transactions.merchantgroupid =5426
group by date (transactions.txncompletetime)

select transactions.userid, extract(month from transactions.txncompletetime), sum(transactions.final_value_USD)
from transactions
where transactions.merchantgroupid =5426
group by transactions.userid, extract(month from transactions.txncompletetime)

/*Question Three Part One*/
select date (transactions.txncompletetime), count(distinct userid) as usercount, count(txnid) as txncount, sum(final_value_USD) as totaltxnusd, sum(price) as totaltxnlocal, avg(final_value_USD) as avgtxnusd, sum(final_value_USD)/count(distinct userid) as avgspenduser 
from transactions
group by date (transactions.txncompletetime) 
ORDER BY sum(final_value_USD) desc
limit 5

/*Question Three Part Two*/
select transactions.countrycode, count(distinct userid) as usercount, count(txnid) as txncount, sum(final_value_USD) as totaltxnusd, sum(price) as totaltxnlocal, avg(final_value_USD) as avgtxnusd, sum(final_value_USD)/count(distinct userid) as avgspenduser 
from transactions
group by transactions.countrycode 

/*Question Three Part Three*/
select transactions.merchantgroupid, count(distinct userid) as usercount, count(txnid) as txncount, sum(final_value_USD) as totaltxnusd, sum(price) as totaltxnlocal, avg(final_value_USD) as avgtxnusd, sum(final_value_USD)/count(distinct userid) as avgspenduser 
from transactions
group by transactions.merchantgroupid 

/*Question Three Part Four*/
select transactions.channeltypeid, count(distinct userid) as usercount, count(txnid) as txncount, sum(final_value_USD) as totaltxnusd, sum(price) as totaltxnlocal, avg(final_value_USD) as avgtxnusd, sum(final_value_USD)/count(distinct userid) as avgspenduser 
from transactions
group by transactions.channeltypeid

/*Question Three Part five*/
select transactions.channelid, count(distinct userid) as usercount, count(txnid) as txncount, sum(final_value_USD) as totaltxnusd, sum(price) as totaltxnlocal, avg(final_value_USD) as avgtxnusd, sum(final_value_USD)/count(distinct userid) as avgspenduser 
from transactions
group by transactions.channelid




ALTER TABLE transactions 
ADD COLUMN fx_rate decimal,
ADD COLUMN final_value decimal;

ALTER TABLE transactions 
RENAME COLUMN final_value TO final_value_USD;

UPDATE transactions 
SET    fx_rate = fx_rates.price 
FROM  fx_rates 
WHERE transactions.key1 = fx_rates.key1

/*table to see spread of users across channels and countries"*/
SELECT 
    "userid" AS "userid",
    COUNT(DISTINCT "countrycode") AS "countrycode_distinct",
    COUNT(DISTINCT "channelid") AS "channelid_distinct",
    COUNT(DISTINCT "channeltypeid") AS "channeltypeid_distinct",
    string_agg( CAST("txnid" AS TEXT), ',') AS "txnid_concat",
    SUM("final_value_usd") AS "final_value_usd_sum",
    COUNT(*) AS "count"
  FROM (
    SELECT 
        "rowid" AS "rowid",
        "countrycode" AS "countrycode",
        "channelid" AS "channelid",
        "channeltypeid" AS "channeltypeid",
        "userid" AS "userid",
        "merchantgroupid" AS "merchantgroupid",
        "txnid" AS "txnid",
        "txncompletetime" AS "txncompletetime",
        "price" AS "price",
        "monthoftxn" AS "monthoftxn",
        "yroftx" AS "yroftx",
        "key1" AS "key1",
        "fx_rate" AS "fx_rate",
        "final_value_usd" AS "final_value_usd"
      FROM "CODA_CURSORY_full_clustering_database"
    ) "dku__beforegrouping"
  GROUP BY "userid"
  
  /*make a table for regression on user spending in December*/
  
 SELECT 
    "Full_clustering_database_by_userid"."userid" AS "userid",
    "Full_clustering_database_by_userid"."countrycode_distinct" AS "countrycode_distinct",
    "Full_clustering_database_by_userid"."channelid_distinct" AS "channelid_distinct",
    "Full_clustering_database_by_userid"."channeltypeid_distinct" AS "channeltypeid_distinct",
    "Full_clustering_database_by_userid"."txnid_concat" AS "txnid_concat",
    "Full_clustering_database_by_userid"."final_value_usd_sum" AS "final_value_usd_sum",
    "Full_clustering_database_by_userid"."count" AS "count",
    "Full_clustering_database_by_userid1"."1_2019_final_value_usd_sum" AS "1_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."1_2020_final_value_usd_sum" AS "1_2020_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."2_2019_final_value_usd_sum" AS "2_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."3_2019_final_value_usd_sum" AS "3_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."4_2019_final_value_usd_sum" AS "4_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."5_2019_final_value_usd_sum" AS "5_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."6_2019_final_value_usd_sum" AS "6_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."7_2019_final_value_usd_sum" AS "7_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."8_2019_final_value_usd_sum" AS "8_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."9_2019_final_value_usd_sum" AS "9_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."10_2019_final_value_usd_sum" AS "10_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."11_2019_final_value_usd_sum" AS "11_2019_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."12_2018_final_value_usd_sum" AS "12_2018_final_value_usd_sum",
    "Full_clustering_database_by_userid1"."12_2019_final_value_usd_sum" AS "12_2019_final_value_usd_sum"
  FROM "CODA_CURSORY_full_clustering_database_by_userid" "Full_clustering_database_by_userid"
  LEFT JOIN "CODA_CURSORY_full_clustering_database_by_userid1" "Full_clustering_database_by_userid1"
    ON "Full_clustering_database_by_userid"."userid" = "Full_clustering_database_by_userid1"."userid"
    
 /*union with the initial Database to see each user attributes*/
    SELECT 
    "Processed_data"."userid" AS "userid",
    "Processed_data"."countrycode_distinct" AS "countrycode_distinct",
    "Processed_data"."channelid_distinct" AS "channelid_distinct",
    "Processed_data"."channeltypeid_distinct" AS "channeltypeid_distinct",
    "Processed_data"."txnid_concat" AS "txnid_concat",
    "Processed_data"."final_value_usd_sum" AS "final_value_usd_sum",
    "Processed_data"."count" AS "count",
    "Processed_data"."1_2019_final_value_usd_sum" AS "1_2019_final_value_usd_sum",
    "Processed_data"."1_2020_final_value_usd_sum" AS "1_2020_final_value_usd_sum",
    "Processed_data"."2_2019_final_value_usd_sum" AS "2_2019_final_value_usd_sum",
    "Processed_data"."3_2019_final_value_usd_sum" AS "3_2019_final_value_usd_sum",
    "Processed_data"."4_2019_final_value_usd_sum" AS "4_2019_final_value_usd_sum",
    "Processed_data"."5_2019_final_value_usd_sum" AS "5_2019_final_value_usd_sum",
    "Processed_data"."6_2019_final_value_usd_sum" AS "6_2019_final_value_usd_sum",
    "Processed_data"."7_2019_final_value_usd_sum" AS "7_2019_final_value_usd_sum",
    "Processed_data"."8_2019_final_value_usd_sum" AS "8_2019_final_value_usd_sum",
    "Processed_data"."9_2019_final_value_usd_sum" AS "9_2019_final_value_usd_sum",
    "Processed_data"."10_2019_final_value_usd_sum" AS "10_2019_final_value_usd_sum",
    "Processed_data"."11_2019_final_value_usd_sum" AS "11_2019_final_value_usd_sum",
    "Processed_data"."12_2018_final_value_usd_sum" AS "12_2018_final_value_usd_sum",
    "Processed_data"."12_2019_final_value_usd_sum" AS "12_2019_final_value_usd_sum",
    "Full_clustering_database"."countrycode" AS "countrycode",
    "Full_clustering_database"."channelid" AS "channelid",
    "Full_clustering_database"."channeltypeid" AS "channeltypeid"
  FROM "CODA_CURSORY_processed_data" "Processed_data"
  LEFT JOIN "CODA_CURSORY_full_clustering_database" "Full_clustering_database"
    ON "Processed_data"."userid" = "Full_clustering_database"."userid"

/*Combine with User Start dates for full view on Customer Lifetime Value*/ 
    SELECT 
    "Final_database_prepared"."userid" AS "userid",
    "Final_database_prepared"."countrycode_distinct" AS "countrycode_distinct",
    "Final_database_prepared"."channelid_distinct" AS "channelid_distinct",
    "Final_database_prepared"."channeltypeid_distinct" AS "channeltypeid_distinct",
    "Final_database_prepared"."final_value_usd_sum" AS "final_value_usd_sum",
    "Final_database_prepared"."count" AS "count",
    "Final_database_prepared"."1_2019_final_value_usd_sum" AS "1_2019_final_value_usd_sum",
    "Final_database_prepared"."1_2020_final_value_usd_sum" AS "1_2020_final_value_usd_sum",
    "Final_database_prepared"."2_2019_final_value_usd_sum" AS "2_2019_final_value_usd_sum",
    "Final_database_prepared"."3_2019_final_value_usd_sum" AS "3_2019_final_value_usd_sum",
    "Final_database_prepared"."4_2019_final_value_usd_sum" AS "4_2019_final_value_usd_sum",
    "Final_database_prepared"."5_2019_final_value_usd_sum" AS "5_2019_final_value_usd_sum",
    "Final_database_prepared"."6_2019_final_value_usd_sum" AS "6_2019_final_value_usd_sum",
    "Final_database_prepared"."7_2019_final_value_usd_sum" AS "7_2019_final_value_usd_sum",
    "Final_database_prepared"."8_2019_final_value_usd_sum" AS "8_2019_final_value_usd_sum",
    "Final_database_prepared"."9_2019_final_value_usd_sum" AS "9_2019_final_value_usd_sum",
    "Final_database_prepared"."10_2019_final_value_usd_sum" AS "10_2019_final_value_usd_sum",
    "Final_database_prepared"."11_2019_final_value_usd_sum" AS "11_2019_final_value_usd_sum",
    "Final_database_prepared"."12_2018_final_value_usd_sum" AS "12_2018_final_value_usd_sum",
    "Final_database_prepared"."12_2019_final_value_usd_sum" AS "12_2019_final_value_usd_sum",
    "Final_database_prepared"."countrycode" AS "countrycode",
    "Final_database_prepared"."channelid" AS "channelid",
    "Final_database_prepared"."channeltypeid" AS "channeltypeid",
    "users"."firsttxncompletetime" AS "firsttxncompletetime"
  FROM "CODA_CURSORY_final_database_prepared" "Final_database_prepared"
  LEFT JOIN "users"
    ON "Final_database_prepared"."userid" = "users"."userid"
