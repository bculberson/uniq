#!/bin/bash

docker run -it -v `pwd`:/usr/local/go/src/github.com/bculberson/uniq -w "/usr/local/go/src/github.com/bculberson/uniq" golang:1.7 bash -c "go build -o bin/uniq main.go"
