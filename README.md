# uniq
go service to guarantee uniqueness with expirations, backed by boltdb

uses raft for leader election and haproxy for leader routing/load balancing

Example usage:


```sh
$ go run main.go -s
$ curl -X POST -i "http://localhost:11111/cns?key=foo&duration=3600s&value=bar"
HTTP/1.1 200 OK
Date: Wed, 15 Mar 2017 16:10:42 GMT
Content-Length: 4
Content-Type: text/plain; charset=utf-8

bar
$ curl -X POST -i "http://localhost:11111/cns?key=foo&duration=60s"
HTTP/1.1 403 Forbidden
Date: Wed, 15 Mar 2017 16:10:43 GMT
Content-Length: 4
Content-Type: text/plain; charset=utf-8

bar

```




