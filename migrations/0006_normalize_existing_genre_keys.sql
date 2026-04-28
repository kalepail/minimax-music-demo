UPDATE songs SET genre_key = 'electronic experimental' WHERE genre_key = 'experimental electronic';
UPDATE songs SET genre_key = 'ambient experimental' WHERE genre_key = 'experimental ambient';
UPDATE songs SET genre_key = 'electronic indie' WHERE genre_key = 'indie electronic';
UPDATE songs SET genre_key = 'avant experimental garde' WHERE genre_key = 'experimental avant garde';
UPDATE songs SET genre_key = 'experimental sea shanty' WHERE genre_key = 'sea shanty experimental';
UPDATE songs SET genre_key = 'folk sea shanty' WHERE genre_key = 'sea shanty folk';
UPDATE songs SET genre_key = 'industrial sea shanty' WHERE genre_key = 'sea shanty industrial';
UPDATE songs SET genre_key = 'chillwave sea shanty' WHERE genre_key = 'sea shanty chillwave';
UPDATE songs SET genre_key = 'electronic sea shanty' WHERE genre_key = 'sea shanty with electronic elements';
UPDATE songs SET genre_key = 'dance electronic world' WHERE genre_key = 'world electronic dance';

PRAGMA optimize;
