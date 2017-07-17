# simulatedUser

This package is part of my Insight Project [nexTop](https://github.com/rentzso/nextop).

This scala library does the following:
- generates multiple users
- assigns a random news (calling `/random` in the Flask API) to each user
- uses the topics in this random document as the initial user topics and generates a set of recommendations (calling `/topics` in the Flask API)
- selects a new random document from the set of recommendations as the next user click. The topics in this document become also part of the user topics
- sends messages with statistics to a Kafka topic
- repeats the previous two steps indefinitely for each user

