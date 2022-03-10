package main

import (
	"context"
	"flag"
	"github.com/golang/protobuf/ptypes/empty"
	"io"
	"math/rand"

	"log"
	"strconv"
	"time"

	pb "github.com/guobinqiu/cuckoofilter/cuckoofilter"
	"google.golang.org/grpc"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
)

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewCuckooFilterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	filterName := "f1"

	createFilter(c, ctx, filterName, 100000000)

	elements := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		elements = append(elements, strconv.Itoa(i))
	}
	insertElements(c, ctx, filterName, elements)

	insertElement(c, ctx, filterName, "100")

	countElements(c, ctx, filterName)

	lookupElement(c, ctx, filterName, "100")

	deleteElement(c, ctx, filterName, "100")
	countElements(c, ctx, filterName)

	lookupElements(c, ctx, filterName, []string{"1", "2", "3"})

	lookupElementsStream(c, ctx, filterName)

	resetFilter(c, ctx, filterName)
	countElements(c, ctx, filterName)

	createFilter(c, ctx, "f2", 100000000)
	listFilters(c, ctx)

	deleteFilter(c, ctx, "f2")
	listFilters(c, ctx)
}

func lookupElementsStream(c pb.CuckooFilterClient, ctx context.Context, filterName string) {
	stream, err := c.LookupElementsStream(ctx)
	if err != nil {
		log.Fatal(err)
	}

	//receive
	waitc := make(chan struct{})
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatal("Failed to receive an element : %v", err)
			}
			log.Println("接收元素：", res.Element)
		}
	}()

	//send
	for i := 0; i < 10; i++ {
		element := strconv.Itoa(rand.Intn(200))
		if err := stream.Send(&pb.LookupElementsStreamRequest{FilterName: filterName, Element: element}); err != nil {
			log.Fatal("Failed to send an element: %v", err)
		}
		log.Println("发送元素：", element)
		time.Sleep(1 * time.Second)
	}
	stream.CloseSend()
	<-waitc
}

func lookupElements(c pb.CuckooFilterClient, ctx context.Context, filterName string, elements []string) ([]string, error) {
	var res *pb.LookupElementsResponse
	res, err := c.LookupElements(ctx, &pb.LookupElementsRequest{FilterName: filterName, Elements: elements})
	if err != nil {
		return nil, err
	}
	log.Println("相交元素：", res.Elements)
	return res.Elements, nil
}

func deleteFilter(c pb.CuckooFilterClient, ctx context.Context, filterName string) error {
	var res *pb.DeleteFilterResponse
	res, err := c.DeleteFilter(ctx, &pb.DeleteFilterRequest{FilterName: filterName})
	if err != nil {
		return err
	}
	log.Println("删除filter：", res.Status.Msg)
	return nil
}

func listFilters(c pb.CuckooFilterClient, ctx context.Context) error {
	var res *pb.ListFiltersResponse
	res, err := c.ListFilters(ctx, new(empty.Empty))
	if err != nil {
		return err
	}
	log.Println("显示所有filter：", res.Filters)
	return nil
}

func insertElements(c pb.CuckooFilterClient, ctx context.Context, filterName string, elements []string) ([]string, error) {
	var res *pb.InsertElementsResponse
	res, err := c.InsertElements(ctx, &pb.InsertElementsRequest{FilterName: filterName, Elements: elements})
	if err != nil {
		return nil, err
	}
	log.Println("插入失败的元素：", res.FailedElements)
	return res.FailedElements, err
}

func resetFilter(c pb.CuckooFilterClient, ctx context.Context, filterName string) error {
	var res *pb.ResetFilterResponse
	res, err := c.ResetFilter(ctx, &pb.ResetFilterRequest{FilterName: filterName})
	if err != nil {
		return err
	}
	log.Println("清空成功？", res.Status.Msg)
	return nil
}

func deleteElement(c pb.CuckooFilterClient, ctx context.Context, filterName string, element string) error {
	var res *pb.DeleteElementResponse
	res, err := c.DeleteElement(ctx, &pb.DeleteElementRequest{FilterName: filterName, Element: element})
	if err != nil {
		return err
	}
	log.Println("删除成功？", res.Status.Msg)
	return nil
}

func lookupElement(c pb.CuckooFilterClient, ctx context.Context, filterName string, element string) error {
	var res *pb.LookupElementResponse
	res, err := c.LookupElement(ctx, &pb.LookupElementRequest{FilterName: filterName, Element: element})
	if err != nil {
		return err
	}
	log.Println("查到了吗？", res.Status.Msg)
	return nil
}

func countElements(c pb.CuckooFilterClient, ctx context.Context, filterName string) error {
	var res *pb.CountElementsResponse
	res, err := c.CountElements(ctx, &pb.CountElementsRequest{FilterName: filterName})
	if err != nil {
		return err
	}
	log.Println("当前元素个数:", res.Len)
	return err
}

func createFilter(c pb.CuckooFilterClient, ctx context.Context, filterName string, capacity uint64) {
	var res *pb.CreateFilterResponse
	res, err := c.CreateFilter(ctx, &pb.CreateFilterRequest{FilterName: filterName, Capacity: capacity})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("创建成功？", res.Status.Msg)
}

func insertElement(c pb.CuckooFilterClient, ctx context.Context, filterName string, element string) error {
	var res *pb.InsertElementResponse
	res, err := c.InsertElement(ctx, &pb.InsertElementRequest{FilterName: filterName, Element: element})
	if err != nil {
		return err
	}
	log.Println("插入成功？", res.Status.Msg, element)
	return nil
}
