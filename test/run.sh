#!/usr/bin/env bash

wrk -c100 -t5 -d30s -s load.lua http://localhost:11110
