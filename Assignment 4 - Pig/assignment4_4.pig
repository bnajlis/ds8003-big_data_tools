tweets = LOAD '/user/root/assgn4/full_clean_text.txt' 
	USING PigStorage('\t') AS (
		user_id:chararray, 
		real_lat:float, 
		real_lon:float, 
		tweet:chararray, 
		mod_lat:int, 
		mod_lon:int
	);
	
cities = LOAD '/user/root/assgn4/cities_clean.txt' AS (
		city_name:chararray, 
		real_lat:float, 
		real_lon:float, 
		mod_lat:int, 
		mod_lon:int
	);
	
combined = JOIN  tweets BY 
	(mod_lat, mod_lon) LEFT OUTER, 
	cities BY (mod_lat, mod_lon) 
	USING 'replicated';
	
distances = FOREACH combined GENERATE 
		tweets::user_id AS user_id, 
		tweets::tweet AS tweet, 
		cities::city_name AS city_name,
		SQRT((tweets::real_lat – cities::real_lat) * (tweets::real_lat – cities::real_lat) + (tweets::real_lon – cities::real_lon) * (tweets::real_lon – cities::real_lon)) as real_dist
	;
	
grouped = GROUP distances BY (user_id, tweet);

min_distance = FOREACH grouped 
	GENERATE group, 
	MIN(distances.real_dist) as distance,
	FLATTEN(distances);
	
min_distance_filtered = FILTER min_distance BY distance == distances::real_dist;

result = FOREACH min_distance_filtered GENERATE
	FLATTEN(group) AS (user_id, tweet),
	distance,
	distances::city_name AS city_name;
	
STORE result INTO '/user/root/assgn4/tweets_by_city' USING PigStorage('\t');