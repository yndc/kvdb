package kvrpc

import (
	"context"
	"fmt"

	"github.com/yndc/kvrpc/pb"
	"google.golang.org/grpc"
)

// Client is the wrapped GRPC client
type Client struct {
	conn   *grpc.ClientConn
	client pb.KVRPCClient
}

// Ping checks the connection
func (c *Client) Ping(ctx context.Context, in *pb.Empty, opts ...grpc.CallOption) (*pb.PingResponse, error) {
	return c.client.Ping(ctx, in, opts...)
}

// Set a value into the KV store
func (c *Client) Set(ctx context.Context, in *pb.SetRequest, opts ...grpc.CallOption) (*pb.SetResponse, error) {
	return c.client.Set(ctx, in, opts...)
}

// Get values from the KV store
func (c *Client) Get(ctx context.Context, in *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetResponse, error) {
	return c.client.Get(ctx, in, opts...)
}

// Del deletes values from the KV store
func (c *Client) Del(ctx context.Context, in *pb.DelRequest, opts ...grpc.CallOption) (*pb.Empty, error) {
	return c.client.Del(ctx, in, opts...)
}

// Close the connection
func (c *Client) Close() error {
	return c.conn.Close()
}

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

	return &Client{
		conn:   conn,
		client: pb.NewKVRPCClient(conn),
	}, nil
}
