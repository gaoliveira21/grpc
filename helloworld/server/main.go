package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"

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

func (gs *GreeterServer) Greet(ctx context.Context, req *pb.GreetRequest) (*pb.GreetReply, error) {
	user := req.GetUser()

	var greeting strings.Builder

	switch req.GetType() {
	case pb.GreetingType_FORMAL:
		greeting.WriteString(fmt.Sprintf("Good day, %s! Welcome to our system.", user.GetName()))
	case pb.GreetingType_INFORMAL:
		greeting.WriteString(fmt.Sprintf("Hey %s! Glad to see you here.", user.GetName()))
	default:
		greeting.WriteString(fmt.Sprintf("Hello %s!", user.GetName()))
	}

	if user.GetIsActive() {
		greeting.WriteString(" It's great to know you're active.")
	} else {
		greeting.WriteString(" Looks like you're currently inactive.")
	}

	return &pb.GreetReply{
		Message: greeting.String(),
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
