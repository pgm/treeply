package treeply

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func InstallCleanup(socketName string) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("Received SIGTERM: Removing %s and exiting...")
		os.Remove(socketName)
		os.Exit(1)
	}()
}

type ReqEnvelope struct {
	Type    string
	Payload json.RawMessage
}

type ListDirReq struct {
	Path string
}

type ListDirResp struct {
	Entries []FileClientDirEntry
}

type OpenReq struct {
	Path string
}

type OpenResp struct {
	FD int
}

type CloseReq struct {
	FD int
}

type CloseResp struct {
}

type ReadReq struct {
	FD     int
	Length int
}

type ReadResp struct {
	Data []byte
}

type SeekReq struct {
	FD     int
	Offset int
	Whence string
}

type SeekResp struct {
	Offset int
}

// type ListDirReq struct {
// 	Path string
// }

// type ListDirResp struct {
// 	Entries []ListDirEntry
// }

type ErrorResp struct {
	Message string
}

type Command struct {
	Type           string
	ReqConstructor func() interface{}
	Invoke         func(interface{}) (interface{}, error)
}

func DispatchReq(client *FileClient, j []byte) interface{} {
	var request ReqEnvelope
	err := json.Unmarshal(j, &request)
	if err != nil {
		log.Fatalln("Unmarshaling %s error:", string(j), err)
	}

	commands := []Command{
		{"open",
			func() interface{} {
				return new(OpenReq)
			},
			func(req interface{}) (interface{}, error) {
				return client.Open(req.(*OpenReq))
			}},
		{"close",
			func() interface{} {
				return new(CloseReq)
			},
			func(req interface{}) (interface{}, error) {
				return client.Close(req.(*CloseReq))
			}},
		{"read",
			func() interface{} {
				return new(ReadReq)
			},
			func(req interface{}) (interface{}, error) {
				return client.Read(req.(*ReadReq))
			}},
		{"listdir",
			func() interface{} {
				return new(ListDirReq)
			},
			func(req interface{}) (interface{}, error) {
				return client.ListDir(req.(*ListDirReq))
			}},
	}

	for _, command := range commands {
		if request.Type == command.Type {
			req := command.ReqConstructor()
			err = json.Unmarshal(request.Payload, req)
			if err != nil {
				log.Printf("Unmarshal payload %s into %s:", request.Payload, req, err)
				return nil
			}

			resp, err := command.Invoke(req)
			if err != nil {
				return &ErrorResp{Message: err.Error()}
			}

			return resp
		}
	}
	log.Printf("unknown request: %s", request.Type)
	return nil
}

func CreateListener(socketName string, fs *FileService) error {
	socket, err := net.Listen("unix", socketName)
	if err != nil {
		return err
	}

	InstallCleanup(socketName)

	log.Printf("Listening on %s", socketName)
	for {
		conn, err := socket.Accept()
		if err != nil {
			return err
		}

		// Handle the connection in a separate goroutine.
		go func(connection net.Conn) {
			log.Printf("Started connection...")

			defer connection.Close()

			client := NewFileClient(fs)

			reader := bufio.NewReader(connection)
			for {
				jsonMessage, isPrefix, err := reader.ReadLine()
				if err == io.EOF {
					break
				}
				if isPrefix {
					log.Fatalf("Line too long")
					return
				}

				log.Printf("Got message: %s", string(jsonMessage))
				response := DispatchReq(client, jsonMessage)
				if response == nil {
					break
				}
				jsonResponse, err := json.Marshal(response)
				if err != nil {
					log.Fatalf("Could not marshal: %s", err)
					return
				}
				jsonResponse = append(jsonResponse, '\n')
				_, err = connection.Write(jsonResponse)

				if err != nil {
					log.Fatalf("Could not write response: %s", err)
					return
				}
			}

			// on reaching EOF, close connection
		}(conn)
	}
}
