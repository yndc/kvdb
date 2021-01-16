package kvrpc

import (
	context "context"

	"github.com/dgraph-io/badger/v3"
	"github.com/rs/zerolog/log"
)

type service struct {
	UnimplementedKVRPCServer
	db *badger.DB
}

func NewService(config *config) *service {
	db, err := badger.Open(
		badger.DefaultOptions(config.path),
	)
	if err != nil {
		log.Fatal().Err(err).Msg("error opening database directory")
	}

	return &service{
		db: db,
	}
}

func (s *service) Close() {
	s.db.Close()
}

// Set writes the given key-value data into the disk
func (s *service) Set(ctx context.Context, in *SetRequest) (*SetResponse, error) {
	s.db.RWMutex.Lock()
	defer s.db.RWMutex.Unlock()

	results := make([]bool, len(in.Values))

	err := s.db.Update(func(txn *badger.Txn) error {
		values := in.GetValues()
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
func (s *service) Get(ctx context.Context, in *GetRequest) (*GetResponse, error) {
	results := make([]*ValueResult, len(in.GetKeys()))

	err := s.db.View(func(txn *badger.Txn) error {
		for i, k := range in.GetKeys() {
			results[i] = &ValueResult{}
			item, err := txn.Get(k)
			if err != nil {
				if err != badger.ErrKeyNotFound {
					return err
				}
			}
			err = item.Value(func(val []byte) error {
				copy(results[i].Value, val)
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
func (s *service) Del(ctx context.Context, in *DelRequest) (*Empty, error) {

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
