data = LOAD '/user/root/assgn4/full_clean_text.txt' USING PigStorage('\t') AS (user_id:chararray, orig_lat:float, orig_lon:float, tweet:chararray, mod_lat:int, mod_lon:int);
tenpctsample = SAMPLE data 0.1;
STORE tenpctsample INTO '/user/root/assgn4/full_text_small.txt' USING PigStorage('\t');