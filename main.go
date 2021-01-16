package kvrpc

import (
	"fmt"
	"net"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

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
