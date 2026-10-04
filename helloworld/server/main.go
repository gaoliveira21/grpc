package main

import (
	"context"
	"log"
	"net"

	pb "github.com/gaoliveira21/grpc-go/proto"
	"google.golang.org/grpc"
)

type GreeterServer struct {
	pb.UnimplementedGreeterServer
}

func (gs *GreeterServer) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{
		Message: "Hello, " + req.GetName(),
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:8080")
	if err != nil {
		log.Fatal("Failed to start server")
	}

	log.Println("Listening on port :8080")

	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &GreeterServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
