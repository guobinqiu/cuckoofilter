# Cuckoofilter gRPC Service

### Run Server 

```
go run main.go
```

### Run Client

```
go run client/go/main.go
```

### Run Unit Test

```
go test cuckoofilter/server
```

### Run Benchmark Test

```
go test -bench=. cuckoofilter/server  -benchmem
```

### API

```
#创建一个过滤器
rpc CreateFilter (CreateFilterRequest) returns (CreateFilterResponse) {}

#删除一个过滤器
rpc DeleteFilter (DeleteFilterRequest) returns (DeleteFilterResponse) {}

#显示所有过滤器
rpc ListFilters (google.protobuf.Empty) returns (ListFiltersResponse) {}

#插入一个元素到指定过滤器
rpc InsertElement (InsertElementRequest) returns (InsertElementResponse) {}

#插入一批元素到指定过滤器
rpc InsertElements (InsertElementsRequest) returns (InsertElementsResponse) {}

#删除一个指定过滤器内的元素
rpc DeleteElement (DeleteElementRequest) returns (DeleteElementResponse) {}

#返回指定过滤器元素个数
rpc CountElements (CountElementsRequest) returns (CountElementsResponse) {}

#删除指定过滤器内所有元素
rpc ResetFilter (ResetFilterRequest) returns (ResetFilterResponse) {}

#查找某一个元素是否存在于指定的过滤器内
rpc LookupElement (LookupElementRequest) returns (LookupElementResponse) {}

#查找该批次的元素是否存在于指定的过滤器内
rpc LookupElements (LookupElementsRequest) returns (LookupElementsResponse) {}

#流式查找元素是否存在于指定的过滤器内
rpc LookupElementsStream (stream LookupElementsStreamRequest) returns (stream LookupElementsStreamResponse) {}
```
