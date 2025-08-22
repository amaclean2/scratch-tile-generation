## Push to aws instructions

```
docker build -t --platform=linux/amd64 weather-lambda .
docker tag weather-lambda:latest 600627347806.dkr.ecr.us-east-1.amazonaws.com/weather-lambda:0.0.27
docker push 600627347806.dkr.ecr.us-east-1.amazonaws.com/weather-lambda:0.0.27

# login command
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 600627347806.dkr.ecr.us-east-1.amazonaws.com
```
