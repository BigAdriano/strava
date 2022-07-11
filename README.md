# strava
Extract activities from Strava by using API, load them to DynamoDB installed locally, use query to get only running activities longer than halfmarathon distance

Prerequisities:
1. DynamoDB installed and working locally - check this: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
2. You have Strava account with activities - you need to allow your application to use Strava API to gather activities - check this: https://developers.strava.com/docs/

As a final result, you will get csv file with list of running-type activities longer than halfmarathon (21.095 km). See example - running.csv
