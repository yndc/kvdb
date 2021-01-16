package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

func main() {
	config := loadConfig()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", config.port))
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to create listener")
	}

	grpcServer := grpc.NewServer()
	kvrpcService := NewService(config)
	RegisterKVRPCServer(grpcServer, kvrpcService)

	signalChan := make(chan os.Signal, 1)
	fatalChan := make(chan error, 1)

	go func() {
		log.Info().Msgf("running at port %d", config.port)
		if err := grpcServer.Serve(lis); err != nil {
			fatalChan <- err
		}
	}()

	signal.Notify(
		signalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGKILL,
	)

	exit := func(code int) {
		go func() {
			<-signalChan
			log.Fatal().Msg("terminated forcefully")
		}()

		kvrpcService.Close()
		os.Exit(code)
	}

	select {
	case <-signalChan:
		log.Info().Msg("shutting down gracefully")
		exit(0)
	case err := <-fatalChan:
		log.Fatal().Err(err).Msg("fatal error")
		exit(1)
	}
}
