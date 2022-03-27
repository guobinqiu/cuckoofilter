# Cuckoofilter gRPC Service

### Run Server 

```
go run cuckoofilter_server/main.go
```

### Run Unit Test

```
go test -v github.com/guobinqiu/cuckoofilter/server
```

### Run Benchmark Test

```
go test -bench=. github.com/guobinqiu/cuckoofilter/server -benchmem
```

### Rebuild

```
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative cuckoofilter/cuckoofilter.proto
```

[Install protoc](https://grpc.io/docs/protoc-installation/#install-using-a-package-manager)

### API

```
#Create a filter
rpc CreateFilter (CreateFilterRequest) returns (CreateFilterResponse) {}

#Delete a filter
rpc DeleteFilter (DeleteFilterRequest) returns (DeleteFilterResponse) {}

#Show all filters
rpc ListFilters (google.protobuf.Empty) returns (ListFiltersResponse) {}

#Insert an element to the specified filter
rpc InsertElement (InsertElementRequest) returns (InsertElementResponse) {}

#Insert a set of elements to the specified filter
rpc InsertElements (InsertElementsRequest) returns (InsertElementsResponse) {}

#Delete an element within a specified filter
rpc DeleteElement (DeleteElementRequest) returns (DeleteElementResponse) {}

#Get the number of elements in the specified filter
rpc CountElements (CountElementsRequest) returns (CountElementsResponse) {}

#Delete all elements in the specified filter
rpc ResetFilter (ResetFilterRequest) returns (ResetFilterResponse) {}

#Find if an element exists in the specified filter
rpc LookupElement (LookupElementRequest) returns (LookupElementResponse) {}

#Get both matched and unmatched elements in the specified filter
rpc LookupElements (LookupElementsRequest) returns (LookupElementsResponse) {}

#Way streaming to find if elements exists in the specified filter
rpc LookupElementsStream (stream LookupElementsStreamRequest) returns (stream LookupElementsStreamResponse) {}
```

### Client Examples

- [go](./cuckoofilter_client/main.go)

- [java](https://github.com/guobinqiu/cuckoofilter-java-client)

### Notes

If the filter judges that the element is in the set, the element may not be in the set; but when it judges that the element is not in the set, it must not be in the set, and the application scenario should allow the existence of false positive.
