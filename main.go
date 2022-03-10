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
	"time"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	dumpPath := "dump"

	s := grpc.NewServer()
	srv := server.NewServer()
	srv.Load(dumpPath)
	pb.RegisterCuckooFilterServer(s, srv)

	quit := make(chan struct{})
	quitTicker := make(chan struct{})

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		//ticker := time.NewTicker(15 * time.Minute)
		isRunning := false
		for {
			select {
			case <-ticker.C:
				fmt.Println(isRunning)
				if !isRunning {
					go func() {
						log.Println("dumping...")
						isRunning = true
						if err := srv.Dump(dumpPath); err != nil {
							log.Printf("failed to dump: %v", err)
						}
						//time.Sleep(10 * time.Second)
						isRunning = false
						log.Println("dumped.")
					}()
				}
			case <-quitTicker:
				ticker.Stop()
				for {
					time.Sleep(100 * time.Millisecond)
					if !isRunning {
						quit <- struct{}{}
						return
					}
				}
			}
		}
	}()

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
	quitTicker <- struct{}{}
	<-quit
}
