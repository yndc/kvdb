package main

import (
	"flag"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type config struct {
	port     int
	path     string
	loglevel string
}

func loadConfig() *config {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	port := flag.Int("p", 9000, "port to listen for requests")
	if port == nil {
		log.Fatal().Msg("port is invalid")
	}
	path := flag.String("d", "./db", "the directory path used for the database")
	if path == nil {
		log.Fatal().Msg("path is invalid")
	}
	loggingLevelStr := flag.String("l", "warn", "logging level (debug | info | warn | error)")
	if loggingLevelStr == nil {
		log.Fatal().Msg("loggingLevelStr is invalid")
	}

	return &config{
		port:     *port,
		path:     *path,
		loglevel: *loggingLevelStr,
	}
}
