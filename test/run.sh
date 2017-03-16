#!/usr/bin/env bash

ab -T 'application/x-www-form-urlencoded' -n 100 -p post.data "http://127.0.0.1:11110/cns"
