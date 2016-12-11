a = LOAD '/user/root/assgn4/full_clean_text.txt' USING PigStorage('\t') AS (user_id:chararray, orig_lat:float, orig_lon:float, tweet:chararray, mod_lat:int, mod_lon:int);
b = FOREACH a GENERATE FLATTEN(TOKENIZE(tweet)) AS token;
c = GROUP b BY token;
d = FOREACH c GENERATE group as token, COUNT_STAR(b) as cnt;
e = ORDER d BY cnt DESC;
f = LIMIT e 3;
DUMP f;