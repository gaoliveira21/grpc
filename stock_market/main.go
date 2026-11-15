package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/gaoliveira21/stock_market/interceptors"
	pb "github.com/gaoliveira21/stock_market/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StockPriceServer struct {
	pb.UnimplementedStockPriceServer
}

func (s *StockPriceServer) GetStockPrice(ctx context.Context, req *pb.StockRequest) (*pb.StockResponse, error) {
	slog.Info("Request received for ", req.GetSymbol(), "")

	price := rand.Float64()

	return &pb.StockResponse{
		Symbol:    req.GetSymbol(),
		Price:     price,
		Timestamp: time.Now().Unix(),
	}, nil
}

func (s *StockPriceServer) GetStockPriceStreaming(req *pb.StockRequest, stream grpc.ServerStreamingServer[pb.StockResponse]) error {
	slog.Info("Request received for ", req.GetSymbol(), "")

	for {
		select {
		case <-stream.Context().Done():
			slog.Info("Client connection closed")
			return nil
		default:
			price := rand.Float64()

			msg := &pb.StockResponse{
				Symbol:    req.GetSymbol(),
				Price:     price,
				Timestamp: time.Now().Unix(),
			}

			stream.Send(msg)
			time.Sleep(time.Second)
		}
	}
}

func (s *StockPriceServer) UpdateStockPriceClientStreaming(stream grpc.ClientStreamingServer[pb.UpdateStockPriceRequest, pb.UpdateStockPriceResponse]) error {
	f, err := os.Create("stockprices.txt")
	if err != nil {
		return err
	}
	defer f.Close()

	stockCount := 0

	for {
		select {
		case <-stream.Context().Done():
			slog.Info("Client connection closed")
			return nil
		default:
			msg, err := stream.Recv()
			if err == io.EOF {
				stream.SendMsg(&pb.UpdateStockPriceResponse{
					Message: fmt.Sprintf("%d Ações atualizadas", stockCount),
				})
				slog.Info("EOF received")
				return nil
			}

			if err != nil {
				slog.Error(err.Error())
				return status.Errorf(codes.Unknown, "The message could not be received")
			}

			slog.Info("Novo preço para a ação", msg.GetSymbol(), ": $ ", strconv.FormatFloat(msg.GetPrice(), 'f', -1, 64), "")

			data := struct {
				Symbol string
				Price  float64
			}{
				Symbol: msg.GetSymbol(),
				Price:  msg.GetPrice(),
			}

			b, err := json.Marshal(data)
			if err != nil {
				return err
			}

			f.Write(b)
			stockCount++
		}
	}
}

func (s *StockPriceServer) GetStockPriceBidirectionalStreaming(stream grpc.BidiStreamingServer[pb.StockRequest, pb.StockResponse]) error {
	for {
		select {
		case <-stream.Context().Done():
			slog.Info("Client connection closed")
			return nil
		default:
			msg, err := stream.Recv()
			if err == io.EOF {
				slog.Info("EOF received")
				return nil
			}

			if err != nil {
				slog.Error(err.Error())
				return status.Errorf(codes.Unknown, "The message could not be received")
			}

			slog.Info("Buscando preço para a ação ", msg.GetSymbol(), "")

			stream.Send(&pb.StockResponse{
				Symbol: msg.GetSymbol(),
				Price:  rand.Float64(),
			})
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:8080")
	if err != nil {
		log.Fatal("Failed to start server")
	}

	log.Println("Listening on port :8080")

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryTraceInterceptor,
			interceptors.UnaryAuthInterceptor,
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamTraceInterceptor,
			interceptors.StreamAuthInterceptor,
		),
	)
	pb.RegisterStockPriceServer(s, &StockPriceServer{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
