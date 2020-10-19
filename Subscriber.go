package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"./pb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Destinationdir string `yaml:"destinationdir"`
	FileSize int `yaml:filesize`
}

const (
	port = "0.0.0.0:50051"
)

type server struct {
}

//protoc ./transfer.proto -I. --go_out=plugins=grpc:.
func (s *server) Upload(stream pb.GuploadService_UploadServer) (err error) {
	var res *pb.Chunk
	destdir,_ := getConfigValues()
	for {
		res, err = stream.Recv()
		//fmt.Println(res.Filename)
		//fmt.Println(res)
		if err == io.EOF {
			err = stream.SendAndClose(&pb.UploadStatus{
				Message: "Upload received with success",
				Code:    pb.UploadStatusCode_Ok,
			})
			if err != nil {
				err = errors.New("failed to send status code")
				return err
			}
			return nil
		}
		_, file := path.Split(res.Filename)
		fo, err := os.Create(destdir + file)
		if err != nil {
			return errors.New("failed to create file")
		}
		// close fo on exit and check for its returned error
		defer func() {
			if err := fo.Close(); err != nil {
				fmt.Println(err)
			}
		}()
		// write a chunk
		if _, err := fo.Write(res.Content); err != nil {
			err = errors.New(err.Error())
			return err
		}
	}
}

func main() {
	/* if len(os.Args) < 1 {
	       fmt.Println("Please Enter HostName and Port")
	       return
	   }
	   url := os.Args[1]*/
	//_,filesize := getConfigValues()

	var options []grpc.ServerOption

	//options = append(options, grpc.MaxSendMsgSize(5*1024*1024*1024*1024), grpc.MaxRecvMsgSize(5*1024*1024*1024*1024))
	options = append(options, grpc.MaxSendMsgSize(5*1024*1024*1024*1024), grpc.MaxRecvMsgSize(5*1024*1024*1024*1024))

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer(options...)

	pb.RegisterGuploadServiceServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
func getConfigValues() (string,int) {
	confContent, err := ioutil.ReadFile("sub_config.yml")
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	conf := &Config{}
	if err := yaml.Unmarshal(confContent, conf); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return conf.Destinationdir,conf.FileSize

}
