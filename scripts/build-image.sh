#!/bin/bash -x

#Exit  immediately  if a simple command (see SHELL GRAMMAR above) exits with a non-zero status.
set -e

# compile sources to zip
rm -f target/universal/*.zip
rm -rf ./svc
sbt dist

mkdir ./svc
unzip -d svc target/universal/*.zip
mv svc/*/* svc/
rm svc/bin/*.bat
mv svc/bin/* svc/bin/start
cp ./scripts/startService.sh svc/bin

docker build -t $APP_IMAGE .
docker push $APP_IMAGE

rm -rf ./svc


