package main

import (
	"flag"
	"fmt"
	pb "github.com/guobinqiu/cuckoofilter/cuckoofilter"
	"github.com/guobinqiu/cuckoofilter/server"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	srv := server.NewServer()
	pb.RegisterCuckooFilterServer(s, srv)

	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-interrupt
	s.Stop()
}
