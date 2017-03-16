# uniq
go service to guarantee uniqueness with expirations, in memory storage

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

To simulate data and load:

```sh
$ curl -X POST -i "http://localhost:11111/load?concurrency=100&number=10000"
HTTP/1.1 200 OK
Date: Thu, 16 Mar 2017 18:12:49 GMT
Content-Length: 247
Content-Type: text/plain; charset=utf-8

{
        "MinimumBatchTime": 0.0032623810000000004,
        "MaximumBatchTime": 0.007492521,
        "AverageBatchTime": 0.008941228470000001,
        "TotalTime": 0.8941228470000001,
        "Batches": 100,
        "BatchSize": 100,
        "Concurrency": 100,
        "Errors": 0,
        "Collisions": 0
}

```



