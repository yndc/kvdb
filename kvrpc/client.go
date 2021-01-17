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

// KeyValue is a pair of key and value
type KeyValue = pb.KeyValue

// ValueResult is a pair of a value and it's existence
type ValueResult = pb.ValueResult

// Ping checks the connection
func (c *Client) Ping(ctx context.Context, opts ...grpc.CallOption) error {
	_, err := c.client.Ping(ctx, &pb.Empty{}, opts...)
	return err
}

// Set a value into the KV store
func (c *Client) Set(ctx context.Context, values []*KeyValue, opts ...grpc.CallOption) ([]bool, error) {
	res, err := c.client.Set(ctx, &pb.SetRequest{
		Values: values,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return res.Result, nil
}

// Get values from the KV store
func (c *Client) Get(ctx context.Context, keys [][]byte, opts ...grpc.CallOption) ([]*ValueResult, error) {
	res, err := c.client.Get(ctx, &pb.GetRequest{
		Keys: keys,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return res.Values, nil
}

// Del deletes values from the KV store
func (c *Client) Del(ctx context.Context, keys [][]byte, opts ...grpc.CallOption) error {
	_, err := c.client.Del(ctx, &pb.DelRequest{
		Keys: keys,
	}, opts...)
	return err
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
