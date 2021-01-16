package kvrpc

import (
	"fmt"
	"net"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

// SayHello implements helloworld.GreeterServer
// func (s *server) SayHello(ctx context.Context, in *service.HelloRequest) (*pb.HelloReply, error) {
// 	log.Printf("Received: %v", in.GetName())
// 	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
// }

func setup() {

}

func main() {
	config := loadConfig()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.port))
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to listen")
	}

	grpcServer := grpc.NewServer()
	kvrpcService := NewService(config)
	defer kvrpcService.Close()

	RegisterKVRPCServer(grpcServer, kvrpcService)
	log.Info().Msgf("running at %d", config.port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal().Err(err).Msgf("failed to serve")
	}
}
