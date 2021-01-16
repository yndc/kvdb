package main

import (
	"fmt"

	"github.com/yndc/kvrpc/pb"
	"google.golang.org/grpc"
)

// Client is the wrapped GRPC client
type Client = pb.KVRPCClient

// ClientOptions is the options to be given into the KVRPC client
type ClientOptions struct {
	Address     string
	DialOptions []grpc.DialOption
}

// NewClient creates a new KVRPC client
func NewClient(opt ClientOptions) (*Client, error) {
	conn, err := grpc.Dial(opt.Address, opt.DialOptions...)
	if err != nil {
		return nil, fmt.Errorf("can't connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVRPCClient(conn)
	return &client, nil
}
