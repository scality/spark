docker stop arcweb
docker rm arcweb
docker build -t arcwebservice -f Dockerfile .
docker run -d --rm  --net=host --name "arcweb" arcwebservice
