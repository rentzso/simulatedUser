# simulatedUser

This Kafka Producer does the following:
- generate multiple users
- assign a random news (calling /random in the Flask API) and collect the user favorite themes, subjects, etc.
- From this small "bootstrap" datastore, continue generating at those companies.
- sends user statistic messages continously to Elasticsearch.
