-- What is the average number of web events of a session from a user on Tech Creator?
SELECT AVG(num_hits) AS avg_web_events
FROM processed_events_sessionized


-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
SELECT host, AVG(num_hits) AS avg_web_events
FROM processed_events_sessionized
GROUP By host