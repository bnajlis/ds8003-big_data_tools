CREATE DATABASE movielens;
USE movielens;

CREATE TABLE movielens.users (
	userid INT,
	age INT,
	gender STRING,
	occupation STRING,
	zipcode STRING
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|';

LOAD DATA INPATH '/user/root/assignment2/u.user'
OVERWRITE INTO TABLE movielens.users;

SELECT * FROM movielens.users LIMIT 10;

CREATE TABLE movielens.data (
	userid INT,
	movieid INT,
	rating INT,
	ts TIMESTAMP
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA INPATH '/user/root/assignment2/u.data'
OVERWRITE INTO TABLE movielens.data;

SELECT * FROM movielens.data LIMIT 10;

-- Find the user id who has rated the most number of movies
SELECT userid, count(*) AS ratings FROM movielens.data GROUP BY userid order by ratings DESC LIMIT 1;

-- Find average rating  received by movie with id 178. 
SELECT AVG(rating) FROM movielens.data WHERE movieid = 178;

--The users belonging to which 3 occupations provided the most number of ratings
SELECT u.userid FROM movielens.users u WHERE u.occupation IN (
SELECT v.occupation FROM(
SELECT u.occupation, count(*) as ratings FROM movielens.data d LEFT JOIN movielens.users u ON u.userid = d.userid GROUP BY u.occupation ORDER BY ratings DESC LIMIT 3
) v
);

--How many unique male users provided at least one rating of 5
SELECT COUNT(DISTINCT d.userid) FROM data d LEFT JOIN users u ON u.userid = d.userid WHERE d.rating = 5 AND u.gender ="M";