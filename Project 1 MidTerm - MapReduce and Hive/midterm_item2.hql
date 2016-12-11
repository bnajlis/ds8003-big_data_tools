SELECT i.movie_name, d.rating
FROM midterm.item i 
	JOIN midterm.data d
	ON d.movie_id = i.movie_id
WHERE d.rating > 3;
