package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"./pb"
	"github.com/fsnotify/fsnotify"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/yaml.v2"
)

//./Publisher one.txt 10.91.170.186:50051
//protoc ./transfer.proto -I. --go_out=plugins=grpc:.
const (
	sentValue = 3000000000 //limit
)

var watcher *fsnotify.Watcher


type Config struct {
	Ip   string `yaml:"ip"`
	Port string `yaml:"port"`
	Watchingdir string `yaml:"watchingdir"`
}

// main
func main() {

	_,_,watchdingir := getConfigValues()
	// creates a new file watcher
	watcher, _ = fsnotify.NewWatcher()
	defer watcher.Close()

	// starting at the root of the project, walk each file/directory searching for
	// directories
	if err := filepath.Walk(watchdingir, watchDir); err != nil {
		fmt.Println("ERROR", err)
	}

	//
	done := make(chan bool)

	//
	go func() {
		for {
			select {
			// watch for events

			case event := <-watcher.Events:
				if(event.Op&fsnotify.Chmod == fsnotify.Chmod){
					//s1,_ := fmt.Printf("Chmod:  %s: %s", event.Op, event.Name)
					//fmt.Println(s1)
					filename := fmt.Sprintf("%s", event.Name)
					fileEvent(filename)
				}
			// watch for errors
			case err := <-watcher.Errors:
				fmt.Println("ERROR", err)
			}
		}
	}()

	<-done
}

func fileEvent(filename string){
	ip,port,workingdir := getConfigValues()
	//address := conf.Ip+":"+conf.Port

	creds, err := credentials.NewClientTLSFromFile("./ncCrypto/cert.pem", "localhost")
	if err != nil {
		log.Fatalln(err)
	}


	var dialOpt []grpc.DialOption
	dialOpt = append(dialOpt, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(5*1024*1024*1024*1024), grpc.MaxCallSendMsgSize(5*1024*1024*1024*1024)),
		grpc.WithTransportCredentials(creds),grpc.WithBlock(),)
	conn, err := grpc.Dial(ip+":"+port, dialOpt...)
	if err != nil {
		log.Fatalf("didn't connect %v", err)
	}
	defer conn.Close()
	c := pb.NewGuploadServiceClient(conn)

	var (
		buf    []byte
		status *pb.UploadStatus
	)
	// open input file

	if _, err := os.Stat(workingdir); err == nil {
		fi, err := os.Open(filename)
		if err != nil {
			fmt.Println(err)
			return
		}
		stat, err := fi.Stat()
		if err != nil {
			return
		}
		// close fi on exit and check for its returned error
		defer func() {
			if err := fi.Close(); err != nil {
				fmt.Println("Not able to open")
				return
			}
		}()

		ctx := context.Background()
		stream, err := c.Upload(ctx)
		if err != nil {
			err = errors.Wrapf(err,
				"failed to create upload stream for file %s", fi)
			return
		}
		defer stream.CloseSend()

		buf = make([]byte, stat.Size())
		for {
			// read a chunk
			n, err := fi.Read(buf)
			if err != nil && err != io.EOF {
				err = errors.Wrapf(err,
					"failed to send chunk via stream")
				return
			}
			if n == 0 {
				break
			}
			var i int64
			for i = 0; i < ((stat.Size() / sentValue) * sentValue); i += sentValue {
				err = stream.Send(&pb.Chunk{
					Content:   buf[i : i+sentValue],
					TotalSize: strconv.FormatInt(stat.Size(), 10),
					Received:  strconv.FormatInt(i+sentValue, 10),
					Filename:  filename,
				})
			}
			if stat.Size()%sentValue > 0 {
				err = stream.Send(&pb.Chunk{
					Content:   buf[((stat.Size() / sentValue) * sentValue):((stat.Size() / sentValue * sentValue) + (stat.Size() % sentValue))],
					TotalSize: strconv.FormatInt(stat.Size(), 10),
					Received:  string(stat.Size() % sentValue),
					Filename:  filename,
				})
			}

			if err != nil {
				err = errors.Wrapf(err,
					"failed to send chunk via stream")
				return
			}
		}
		status, err = stream.CloseAndRecv()
		if err != nil {
			err = errors.Wrapf(err,
				"failed to receive upstream status response")
			return
		}

		if status.Code != pb.UploadStatusCode_Ok {
			err = errors.Errorf(
				"upload failed - msg: %s",
				status.Message)
			return
		}

		return
	} else if os.IsNotExist(err) {
		fmt.Println("file doesnot exist ")

	}
}
// watchDir gets run as a walk func, searching for directories to add watchers to
func watchDir(path string, fi os.FileInfo, err error) error {

	// since fsnotify can watch all the files in a directory, watchers only need
	// to be added to each nested directory
	if fi.Mode().IsDir() {
		return watcher.Add(path)
	}

	return nil
}
func getConfigValues() (string,string,string){
	confContent, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	conf := &Config{}
	if err := yaml.Unmarshal(confContent, conf); err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	return conf.Ip,conf.Port,conf.Watchingdir

}
