package main

import (
	context "context"

	"github.com/dgraph-io/badger/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Service is gRPC service for KVRPC
type Service struct {
	UnimplementedKVRPCServer
	db *badger.DB
}

type zeroLogger struct {
	logger zerolog.Logger
}

func newZeroLogger(level zerolog.Level) *zeroLogger {
	return &zeroLogger{
		logger: log.Logger.Level(level),
	}
}

func (l *zeroLogger) Errorf(str string, args ...interface{}) {
	l.logger.Error().Msgf(str, args...)
}

func (l *zeroLogger) Warningf(str string, args ...interface{}) {
	l.logger.Warn().Msgf(str, args...)
}

func (l *zeroLogger) Infof(str string, args ...interface{}) {
	l.logger.Info().Msgf(str, args...)
}

func (l *zeroLogger) Debugf(str string, args ...interface{}) {
	l.logger.Debug().Msgf(str, args...)
}

// NewService creates a new KVRPC service
func NewService(config *config) *Service {
	opt := badger.DefaultOptions(config.path)
	switch config.loglevel {
	case "debug":
		opt = opt.WithLogger(newZeroLogger(zerolog.DebugLevel))
	case "info":
		opt = opt.WithLogger(newZeroLogger(zerolog.InfoLevel))
	case "warn":
		opt = opt.WithLogger(newZeroLogger(zerolog.WarnLevel))
	case "error":
		opt = opt.WithLogger(newZeroLogger(zerolog.ErrorLevel))
	default:
		opt = opt.WithLogger(newZeroLogger(zerolog.InfoLevel))
	}
	db, err := badger.Open(opt)
	if err != nil {
		log.Fatal().Err(err).Msg("error opening database directory")
	}

	return &Service{
		db: db,
	}
}

// Close the service
func (s *Service) Close() {
	s.db.Close()
}

// Set writes the given key-value data into the disk
func (s *Service) Set(ctx context.Context, in *SetRequest) (*SetResponse, error) {
	results := make([]bool, len(in.Values))

	err := s.db.Update(func(txn *badger.Txn) error {
		values := in.Values
		for i, v := range values {
			err := txn.Set(v.Key, v.Value)
			if err != nil {
				return err
			}
			results[i] = true
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &SetResponse{
		Result: results,
	}, nil
}

// Get retrieves the data specified by the given keys
func (s *Service) Get(ctx context.Context, in *GetRequest) (*GetResponse, error) {
	results := make([]*ValueResult, len(in.Keys))
	err := s.db.View(func(txn *badger.Txn) error {
		for i, k := range in.Keys {
			results[i] = &ValueResult{}
			item, err := txn.Get(k)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					results[i].Exists = false
					continue
				}
				return err
			}
			err = item.Value(func(val []byte) error {
				dst := make([]byte, len(val))
				copy(dst, val)
				results[i].Value = dst
				results[i].Exists = true

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &GetResponse{
		Values: results,
	}, nil
}

// Del deletes the data with the given keys
func (s *Service) Del(ctx context.Context, in *DelRequest) (*Empty, error) {

	err := s.db.Update(func(txn *badger.Txn) error {
		keys := in.GetKeys()
		for _, k := range keys {
			err := txn.Delete(k)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &Empty{}, nil
}
