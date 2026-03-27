#!/bin/bash -e

rm -rf dist
mkdir -p dist

go mod tidy
GOOS=darwin go build -o dist/message-generators/darwin message_generator.go
GOOS=linux go build -o dist/message-generators/linux message_generator.go
GOOS=windows go build -o dist/message-generators/windows.exe message_generator.go

cp docker-compose.yml dist/
cp DOC.md dist/README.md

echo "Date: $(date)" >> dist/README.md
