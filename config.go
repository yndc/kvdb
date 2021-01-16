package kvrpc

import (
	"flag"

	"github.com/rs/zerolog/log"
)

type config struct {
	port int
	path string
}

func loadConfig() *config {
	port := flag.Int("p", 9625, "port to listen for requests")
	if port == nil {
		log.Fatal().Msg("port is invalid")
	}
	path := flag.String("d", "./db", "the directory path used for the database")
	if path == nil {
		log.Fatal().Msg("path is invalid")
	}
	return &config{
		port: *port,
		path: *path,
	}
}
