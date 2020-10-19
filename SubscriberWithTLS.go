package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"./pb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"crypto/tls"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)


type Config struct {
	Destinationdir string `yaml:"destinationdir"`
}


const (
	port = "0.0.0.0:50051"
)

type server struct {
}

//protoc ./transfer.proto -I. --go_out=plugins=grpc:.
func (s *server) Upload(stream pb.GuploadService_UploadServer) (err error) {
	destinationdir := getConfigValues()
	var res *pb.Chunk
	for {
		res, err = stream.Recv()
		fmt.Println(res.Filename)
		fmt.Println(res)
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
		fo, err := os.Create(destinationdir + file)
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
	cert, err := tls.LoadX509KeyPair("./ncCrypto/cert.pem", "./ncCrypto/key.pem")
	if err != nil {
		log.Fatalln(err)
	}
	var options []grpc.ServerOption
	options = append(options, grpc.MaxSendMsgSize(5*1024*1024*1024*1024), grpc.MaxRecvMsgSize(5*1024*1024*1024*1024),grpc.Creds(credentials.NewServerTLSFromCert(&cert)))

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
func getConfigValues() (string){
	confContent, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	conf := &Config{}
	if err := yaml.Unmarshal(confContent, conf); err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	return conf.Destinationdir

}

