docker stop sofs
docker rm sofs
docker build --network host -t sofswebservice -f Dockerfile .
docker run -d --rm  --net=host --name "sofs" sofswebservice
