#!/usr/bin/env bash

wrk -c100 -t5 -d5s -s load.lua http://localhost:11111
