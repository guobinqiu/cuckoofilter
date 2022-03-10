package server

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	pb "github.com/guobinqiu/cuckoofilter/cuckoofilter"
	"github.com/panmari/cuckoofilter"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestCreateFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	var res *pb.CreateFilterResponse
	res, _ = s.CreateFilter(ctx, &pb.CreateFilterRequest{FilterName: "aaa", Capacity: 100})
	assert.Equal(t, res.Status, StatusOK)
}

func TestCreateFilterAlreadyExist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	s.Filters["aaa"] = cuckoo.NewFilter(100)

	var res *pb.CreateFilterResponse
	res, _ = s.CreateFilter(ctx, &pb.CreateFilterRequest{FilterName: "aaa", Capacity: 100})
	assert.Equal(t, res.Status, StatusFilterAlreadyExist)
}

func TestDeleteFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	s.Filters["aaa"] = cuckoo.NewFilter(100)

	var res *pb.DeleteFilterResponse
	res, _ = s.DeleteFilter(ctx, &pb.DeleteFilterRequest{FilterName: "aaa"})

	assert.Equal(t, res.Status, StatusOK)
}

func TestDeleteFilterNoFilterFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	s.Filters["aaa"] = cuckoo.NewFilter(100)

	var res *pb.DeleteFilterResponse
	res, _ = s.DeleteFilter(ctx, &pb.DeleteFilterRequest{FilterName: "bbb"})

	assert.Equal(t, res.Status, StatusNoFilterFound)
}

func TestListFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()

	var res *pb.ListFiltersResponse
	res, _ = s.ListFilters(ctx, new(empty.Empty))

	assert.Len(t, res.Filters, 0)
}

func TestListFilterEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	s.Filters["aaa"] = cuckoo.NewFilter(100)
	s.Filters["bbb"] = cuckoo.NewFilter(100)

	var res *pb.ListFiltersResponse
	res, _ = s.ListFilters(ctx, new(empty.Empty))

	assert.Len(t, res.Filters, 2)
}

func TestInsertElement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	s.Filters["aaa"] = cuckoo.NewFilter(100)

	var res *pb.InsertElementResponse
	res, _ = s.InsertElement(ctx, &pb.InsertElementRequest{FilterName: "aaa", Element: "jack"})

	assert.Equal(t, res.Status, StatusOK)
}

func TestInsertElementNoFilterFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	s.Filters["aaa"] = cuckoo.NewFilter(100)

	var res *pb.InsertElementResponse
	res, _ = s.InsertElement(ctx, &pb.InsertElementRequest{FilterName: "bbb", Element: "jack"})

	assert.Equal(t, res.Status, StatusNoFilterFound)
}

func TestDeleteElement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))

	var res *pb.DeleteElementResponse
	res, _ = s.DeleteElement(ctx, &pb.DeleteElementRequest{FilterName: "aaa", Element: "jack"})

	assert.Equal(t, res.Status, StatusOK)
}

func TestDeleteElementNoElementFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))

	var res *pb.DeleteElementResponse
	res, _ = s.DeleteElement(ctx, &pb.DeleteElementRequest{FilterName: "aaa", Element: "mary"})

	assert.Equal(t, res.Status, StatusNoElementFound)
}

func TestDeleteElementNoFilterFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))

	var res *pb.DeleteElementResponse
	res, _ = s.DeleteElement(ctx, &pb.DeleteElementRequest{FilterName: "bbb", Element: "jack"})

	assert.Equal(t, res.Status, StatusNoFilterFound)
}

func TestCountElements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))
	filter.Insert([]byte("mary"))

	var res *pb.CountElementsResponse
	res, _ = s.CountElements(ctx, &pb.CountElementsRequest{FilterName: "aaa"})

	assert.True(t, res.Len == 2)
}

func TestCountElementsNoFilterFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))
	filter.Insert([]byte("mary"))

	var res *pb.CountElementsResponse
	res, _ = s.CountElements(ctx, &pb.CountElementsRequest{FilterName: "bbb"})

	assert.Equal(t, res.Status, StatusNoFilterFound)
}

func TestResetFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))
	filter.Insert([]byte("mary"))

	s.ResetFilter(ctx, &pb.ResetFilterRequest{FilterName: "aaa"})

	assert.Zero(t, filter.Count())
}

func TestResetFilterNoFilterFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))
	filter.Insert([]byte("mary"))

	var res *pb.ResetFilterResponse
	res, _ = s.ResetFilter(ctx, &pb.ResetFilterRequest{FilterName: "bbb"})

	assert.Equal(t, res.Status, StatusNoFilterFound)
}

func TestLookupElement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))

	var res *pb.LookupElementResponse
	res, _ = s.LookupElement(ctx, &pb.LookupElementRequest{FilterName: "aaa", Element: "jack"})

	assert.Equal(t, res.Status, StatusOK)
}

func TestLookupElementNoFilterFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))

	var res *pb.LookupElementResponse
	res, _ = s.LookupElement(ctx, &pb.LookupElementRequest{FilterName: "bbb", Element: "jack"})

	assert.Equal(t, res.Status, StatusNoFilterFound)
}

func TestLookupElementNoElementFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))

	var res *pb.LookupElementResponse
	res, _ = s.LookupElement(ctx, &pb.LookupElementRequest{FilterName: "aaa", Element: "mary"})

	assert.Equal(t, res.Status, StatusNoElementFound)
}

func TestLookupElements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))

	var res *pb.LookupElementsResponse
	res, _ = s.LookupElements(ctx, &pb.LookupElementsRequest{FilterName: "aaa", Elements: []string{"jack"}})

	assert.Equal(t, res.Status, StatusOK)
}

func TestLookupElementsNoFilterFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte("jack"))

	var res *pb.LookupElementsResponse
	res, _ = s.LookupElements(ctx, &pb.LookupElementsRequest{FilterName: "bbb", Elements: []string{"jack"}})

	assert.Equal(t, res.Status, StatusNoFilterFound)
}

func TestLookupElementsOverLimitation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	filter := cuckoo.NewFilter(100)
	s.Filters["aaa"] = filter
	filter.Insert([]byte(""))

	var res *pb.LookupElementsResponse
	elements := make([]string, 5000)
	res, _ = s.LookupElements(ctx, &pb.LookupElementsRequest{FilterName: "aaa", Elements: elements})
	assert.Equal(t, res.Status, StatusOK)

	elements = make([]string, 5001)
	res, _ = s.LookupElements(ctx, &pb.LookupElementsRequest{FilterName: "aaa", Elements: elements})
	assert.Equal(t, res.Status, StatusOverLimitation)
}

func TestInsertElements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := NewServer()
	s.Filters["aaa"] = cuckoo.NewFilter(100)

	var res *pb.InsertElementsResponse
	res, _ = s.InsertElements(ctx, &pb.InsertElementsRequest{FilterName: "aaa", Elements: []string{"jack"}})

	assert.Equal(t, res.Status, StatusOK)
}

func TestLookupElementsStream(t *testing.T) {

}

func TestDumpAndLoad(t *testing.T) {
	f1 := cuckoo.NewFilter(3)
	f1.Insert([]byte("a"))
	f1.Insert([]byte("b"))
	f1.Insert([]byte("c"))

	f2 := cuckoo.NewFilter(3)
	f2.Insert([]byte("x"))
	f2.Insert([]byte("y"))
	f2.Insert([]byte("z"))

	s := NewServer()
	s.Filters["aaa"] = f1
	s.Filters["bbb"] = f2

	dir := "dump"
	assert.NoError(t, s.Dump(dir))

	s = NewServer()
	s.Load(dir)

	assert.True(t, s.Filters["aaa"].Lookup([]byte("a")))
	assert.True(t, s.Filters["aaa"].Lookup([]byte("b")))
	assert.True(t, s.Filters["aaa"].Lookup([]byte("c")))
	assert.False(t, s.Filters["aaa"].Lookup([]byte("d")))

	assert.True(t, s.Filters["bbb"].Lookup([]byte("x")))
	assert.True(t, s.Filters["bbb"].Lookup([]byte("y")))
	assert.True(t, s.Filters["bbb"].Lookup([]byte("z")))
	assert.False(t, s.Filters["bbb"].Lookup([]byte("w")))

	os.RemoveAll(dir)
}

func BenchmarkLoad1Million(b *testing.B)   { load("aaa", 1000000) }
func BenchmarkLoad10Million(b *testing.B)  { load("aaa", 10000000) }
func BenchmarkLoad100Million(b *testing.B) { load("aaa", 100000000) }

//func BenchmarkLoad1000Million(b *testing.B) { load("aaa", 1000000000) }

func BenchmarkLookupElement1Million(b *testing.B)   { benchmarkLookupElement("aaa", 1000000, b) }
func BenchmarkLookupElement10Million(b *testing.B)  { benchmarkLookupElement("aaa", 10000000, b) }
func BenchmarkLookupElement100Million(b *testing.B) { benchmarkLookupElement("aaa", 100000000, b) }

//func BenchmarkLookupElement1000Million(b *testing.B) { benchmarkLookupElement("aaa", 1000000000, b) }

func benchmarkLookupElement(filterName string, filterCapacity uint, b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, lastElement := load(filterName, filterCapacity)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.LookupElement(ctx, &pb.LookupElementRequest{FilterName: filterName, Element: lastElement})
	}
}

func load(filterName string, filterCapacity uint) (s *cuckooFilterServer, lastElement string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s = NewServer()
	s.Filters[filterName] = cuckoo.NewFilter(filterCapacity)

	var i uint
	for i = 0; i < filterCapacity-1; i++ {
		s.InsertElement(ctx, &pb.InsertElementRequest{FilterName: filterName, Element: uuid.New().String()})
	}

	lastElement = uuid.New().String()
	s.InsertElement(ctx, &pb.InsertElementRequest{FilterName: filterName, Element: lastElement})

	return
}
