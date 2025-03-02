package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"net"
	"os"
	"raft-kv_store/pkg/rkv/pb"
	"time"
)

const (
	getMode = "get"
	setMode = "set"
	delMode = "del"
)

func main() {
	mode := parseArgs()

	conn, err := getConnection(mode.addr)
	if err != nil {
		fmt.Printf("getConnection err: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	switch mode.name {
	case getMode:
		get(conn, &pb.GetRequest{Key: mode.params.(string)})
	case setMode:
		set(conn, &pb.SetRequest{Key: mode.params.(keyValuePair).key, Value: mode.params.(keyValuePair).value})
	case delMode:
		del(conn, &pb.DeleteRequest{Key: mode.params.(string)})
	}
}

func getConnection(address string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})), grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
		return net.DialTimeout("tcp", addr, 60*time.Second)
	}))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func get(conn *grpc.ClientConn, req *pb.GetRequest) {
	client := pb.NewKVStoreRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	reply, err := client.Get(ctx, req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("成功 :%v\n", reply.Success)
	fmt.Printf("值为 :%s\n", reply.Value)
	fmt.Printf("在Node%d运行\n", reply.NodeID)
}

func set(conn *grpc.ClientConn, req *pb.SetRequest) {
	client := pb.NewKVStoreRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reply, err := client.Set(ctx, req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("成功:%v\n", reply.Success)
	fmt.Printf("在Node%d运行\n", reply.NodeID)
}

func del(conn *grpc.ClientConn, req *pb.DeleteRequest) {
	client := pb.NewKVStoreRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reply, err := client.Delete(ctx, req)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("成功:%v\n", reply.Success)
	fmt.Printf("在Node%d运行\n", reply.NodeID)
}

type runMode struct {
	name   string
	addr   string
	params interface{}
}

type keyValuePair struct {
	key   string
	value string
}

func parseArgs() runMode {
	if len(os.Args) < 2 {
		fmt.Println("Not enough arguments")
		printUsage()
		os.Exit(1)
	}

	mode := runMode{
		name: os.Args[1],
	}

	args := os.Args[2:]
	switch mode.name {
	case getMode:
		key := ""
		getCmd := flag.NewFlagSet(getMode, flag.ExitOnError)
		getCmd.StringVar(&mode.addr, "address", "", "rpc endpoint")
		getCmd.StringVar(&key, "key", "", "kv store key to get")
		getCmd.Parse(args)
		mode.params = key
	case setMode:
		kvp := keyValuePair{}
		setCmd := flag.NewFlagSet(setMode, flag.ExitOnError)
		setCmd.StringVar(&mode.addr, "address", "", "rpc endpoint")
		setCmd.StringVar(&kvp.key, "key", "", "kv store key to set")
		setCmd.StringVar(&kvp.value, "value", "", "kv store value to set")
		setCmd.Parse(args)
		mode.params = kvp
	case delMode:
		key := ""
		delCmd := flag.NewFlagSet(delMode, flag.ExitOnError)
		delCmd.StringVar(&mode.addr, "address", "", "rpc endpoint")
		delCmd.StringVar(&key, "key", "", "kv store key to delete")
		delCmd.Parse(args)
		mode.params = key
	default:
		mode.name = ""
	}

	if mode.name == "" {
		printUsage()
		log.Fatalln("Unsupported mode")
	}

	if mode.addr == "" {
		printUsage()
		log.Fatalln("address cannot be empty")
	}

	return mode
}

func printUsage() {
	fmt.Println("Usage of rkvclient:")
	fmt.Println("\trkvclient <mode> -address <nodeaddress> <othermodeparams>")
	fmt.Println("Supported modes:")
	fmt.Println("\tset       -address <address> -key <key> -value <value>")
	fmt.Println("\tget       -address <address> -key <key>")
	fmt.Println("\tdel       -address <address> -key <key>")
	fmt.Println()

}
