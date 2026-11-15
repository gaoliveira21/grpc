package interceptors

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func UnaryTraceInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	m, err := handler(metadata.AppendToOutgoingContext(ctx, "traceId", uuid.NewString()), req)

	return m, err
}

func StreamTraceInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, newWrappedStream(ss, metadata.AppendToOutgoingContext(ss.Context(), "traceId", uuid.NewString())))

	return err
}
