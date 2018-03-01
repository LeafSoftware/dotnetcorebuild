ORG=leafsoftware
IMAGE=dotnetcorebuild
VERSION=`cat VERSION`

docker build -t $ORG/$IMAGE:latest .
docker tag $ORG/$IMAGE:latest $ORG/$IMAGE:$VERSION
docker push $ORG/$IMAGE:latest
docker push $ORG/$IMAGE:$VERSION
