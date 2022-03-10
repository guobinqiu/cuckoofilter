package server

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	pb "github.com/guobinqiu/cuckoofilter/cuckoofilter"
	"github.com/panmari/cuckoofilter"
	"io"
	"io/ioutil"
	"os"
	"sync"
)

const maxElementCount = 5000

var (
	StatusOK                 = &pb.Status{Code: 0, Msg: "OK"}
	StatusNoFilterFound      = &pb.Status{Code: 1, Msg: "No filter found."}
	StatusInsertionFailed    = &pb.Status{Code: 2, Msg: "Insertion failed. To increase success rate of inserts, create a larger filter."}
	StatusNoElementFound     = &pb.Status{Code: 3, Msg: "No element found."}
	StatusOverLimitation     = &pb.Status{Code: 4, Msg: fmt.Sprintf("Elements amount over %d limitation", maxElementCount)}
	StatusFilterAlreadyExist = &pb.Status{Code: 5, Msg: "Filter already exist"}
)

type cuckooFilterServer struct {
	pb.UnimplementedCuckooFilterServer
	Filters  map[string]*cuckoo.Filter
	mu       sync.Mutex
	dumpWait chan struct{}
}

func NewServer() *cuckooFilterServer {
	s := &cuckooFilterServer{Filters: make(map[string]*cuckoo.Filter)}
	return s
}

func (s *cuckooFilterServer) CreateFilter(ctx context.Context, req *pb.CreateFilterRequest) (*pb.CreateFilterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	filter, ok := s.Filters[req.FilterName]
	if ok {
		return &pb.CreateFilterResponse{Status: StatusFilterAlreadyExist}, nil
	}
	filter = cuckoo.NewFilter(uint(req.Capacity))
	s.Filters[req.FilterName] = filter
	return &pb.CreateFilterResponse{Status: StatusOK}, nil
}

func (s *cuckooFilterServer) DeleteFilter(ctx context.Context, req *pb.DeleteFilterRequest) (*pb.DeleteFilterResponse, error) {
	_, ok := s.Filters[req.FilterName]
	if !ok {
		return &pb.DeleteFilterResponse{Status: StatusNoFilterFound}, nil
	}
	delete(s.Filters, req.FilterName)
	return &pb.DeleteFilterResponse{Status: StatusOK}, nil
}

func (s *cuckooFilterServer) ListFilters(ctx context.Context, e *empty.Empty) (*pb.ListFiltersResponse, error) {
	filterNames := make([]string, 0, len(s.Filters))
	for key := range s.Filters {
		filterNames = append(filterNames, key)
	}
	return &pb.ListFiltersResponse{Status: StatusOK, Filters: filterNames}, nil
}

func (s *cuckooFilterServer) InsertElement(ctx context.Context, req *pb.InsertElementRequest) (*pb.InsertElementResponse, error) {
	filter, ok := s.Filters[req.FilterName]
	if !ok {
		return &pb.InsertElementResponse{Status: StatusNoFilterFound}, nil
	}
	if !filter.Insert([]byte(req.Element)) {
		return &pb.InsertElementResponse{Status: StatusInsertionFailed}, nil
	}
	return &pb.InsertElementResponse{Status: StatusOK}, nil
}

func (s *cuckooFilterServer) InsertElements(ctx context.Context, req *pb.InsertElementsRequest) (*pb.InsertElementsResponse, error) {
	filter, ok := s.Filters[req.FilterName]
	if !ok {
		return &pb.InsertElementsResponse{Status: StatusNoFilterFound}, nil
	}
	if len(req.Elements) > maxElementCount {
		return &pb.InsertElementsResponse{Status: StatusOverLimitation}, nil
	}
	var failedElements = make([]string, 0, maxElementCount)
	for _, element := range req.Elements {
		if !filter.Insert([]byte(element)) {
			failedElements = append(failedElements, element)
		}
	}
	if len(failedElements) > 0 {
		return &pb.InsertElementsResponse{Status: StatusNoFilterFound, FailedElements: failedElements}, nil
	}
	return &pb.InsertElementsResponse{Status: StatusOK}, nil
}

func (s *cuckooFilterServer) DeleteElement(ctx context.Context, req *pb.DeleteElementRequest) (*pb.DeleteElementResponse, error) {
	filter, ok := s.Filters[req.FilterName]
	if !ok {
		return &pb.DeleteElementResponse{Status: StatusNoFilterFound}, nil
	}
	if !filter.Delete([]byte(req.Element)) {
		return &pb.DeleteElementResponse{Status: StatusNoElementFound}, nil
	}
	return &pb.DeleteElementResponse{Status: StatusOK}, nil
}

func (s *cuckooFilterServer) CountElements(ctx context.Context, req *pb.CountElementsRequest) (*pb.CountElementsResponse, error) {
	filter, ok := s.Filters[req.FilterName]
	if !ok {
		return &pb.CountElementsResponse{Status: StatusNoFilterFound}, nil
	}
	return &pb.CountElementsResponse{Status: StatusOK, Len: uint64(filter.Count())}, nil
}

func (s *cuckooFilterServer) ResetFilter(ctx context.Context, req *pb.ResetFilterRequest) (*pb.ResetFilterResponse, error) {
	filter, ok := s.Filters[req.FilterName]
	if !ok {
		return &pb.ResetFilterResponse{Status: StatusNoFilterFound}, nil
	}
	filter.Reset()
	return &pb.ResetFilterResponse{Status: StatusOK}, nil
}

func (s *cuckooFilterServer) LookupElement(ctx context.Context, req *pb.LookupElementRequest) (*pb.LookupElementResponse, error) {
	filter, ok := s.Filters[req.FilterName]
	if !ok {
		return &pb.LookupElementResponse{Status: StatusNoFilterFound}, nil
	}
	if !filter.Lookup([]byte(req.Element)) {
		return &pb.LookupElementResponse{Status: StatusNoElementFound}, nil
	}
	return &pb.LookupElementResponse{Status: StatusOK}, nil
}

func (s *cuckooFilterServer) LookupElements(ctx context.Context, req *pb.LookupElementsRequest) (*pb.LookupElementsResponse, error) {
	filter, ok := s.Filters[req.FilterName]
	if !ok {
		return &pb.LookupElementsResponse{Status: StatusNoFilterFound}, nil
	}
	if len(req.Elements) > maxElementCount {
		return &pb.LookupElementsResponse{Status: StatusOverLimitation}, nil
	}
	var matchedElements = make([]string, 0, maxElementCount)
	for _, element := range req.Elements {
		if filter.Lookup([]byte(element)) {
			matchedElements = append(matchedElements, element)
		}
	}
	if len(matchedElements) == 0 {
		return &pb.LookupElementsResponse{Status: StatusNoElementFound}, nil
	}
	return &pb.LookupElementsResponse{Status: StatusOK, Elements: matchedElements}, nil
}

func (s *cuckooFilterServer) LookupElementsStream(stream pb.CuckooFilter_LookupElementsStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		filter, ok := s.Filters[req.FilterName]
		if ok && filter.Lookup([]byte(req.Element)) {
			if err := stream.Send(&pb.LookupElementsStreamResponse{Element: req.Element}); err != nil {
				return err
			}
		}
	}
}

func (s *cuckooFilterServer) Dump(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}
	for k, v := range s.Filters {
		f, err := ioutil.TempFile(dir, k+"-*")
		if err != nil {
			return err
		}

		if _, err := f.Write(v.Encode()); err != nil {
			return err
		}

		f.Close()
		os.Rename(f.Name(), dir+"/"+k)
	}
	return nil
}

func (s *cuckooFilterServer) Load(dir string) error {
	fileInfoList, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	for i := range fileInfoList {
		b, err := ioutil.ReadFile(dir + "/" + fileInfoList[i].Name())
		if err != nil {
			return err
		}

		f, err := cuckoo.Decode(b)
		if err != nil {
			return err
		}

		s.Filters[fileInfoList[i].Name()] = f
	}
	return nil
}
