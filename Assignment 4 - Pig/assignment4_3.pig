a = LOAD '/user/root/assgn4/cities_clean.txt' AS (city_name:chararray, real_lat:float, real_lon:float, mod_lat:int, mod_lon_int);
b = GROUP a ALL;
c = FOREACH b GENERATE COUNT(a);
DUMP c;