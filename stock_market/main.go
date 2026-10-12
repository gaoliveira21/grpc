package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"time"

	pb "github.com/gaoliveira21/stock_market/protos"
	"google.golang.org/grpc"
)

type StockPriceServer struct {
	pb.UnimplementedStockPriceServer
}

func (s *StockPriceServer) GetStockPrice(ctx context.Context, req *pb.StockRequest) (*pb.StockResponse, error) {
	price := rand.Float64()

	return &pb.StockResponse{
		Symbol:    req.GetSymbol(),
		Price:     price,
		Timestamp: time.Now().Unix(),
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:8080")
	if err != nil {
		log.Fatal("Failed to start server")
	}

	log.Println("Listening on port :8080")

	s := grpc.NewServer()
	pb.RegisterStockPriceServer(s, &StockPriceServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
