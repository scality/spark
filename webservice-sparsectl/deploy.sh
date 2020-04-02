docker stop sofs
docker rm sofs
docker build --network host -t sofswebservice -f Dockerfile .
docker run -d  -v /ring/fs/RTL2/sfused.conf:/home/website/bin/sfused.conf:ro --rm  --net=host --name "sofs" sofswebservice
