package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"
)

const (
	RECV_BUF_LEN     = 200
	EVENT_BUFFER_LEN = 1000
)

type Event struct {
	timestamp    int64
	client_ip    int32
	event_type   string
	user_agent   string
	country_code [2]byte
	region_code  [3]byte
	post_code    string
	api_key      string
	url          string
	user_uid     string
	user_quizzed bool
}

var event_buffer []Event

func create_map(file string, file_size int, create bool) []byte {
	var mode int
	var mmap []byte
	var map_file *os.File
	var err error

	if create {
		map_file, err = os.Create(file)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		_, err = map_file.Seek(int64(file_size-1), 0)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		_, err = map_file.Write([]byte{0})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		mode = syscall.PROT_WRITE
	} else {
		map_file, err = os.Open("test.dat")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		mode = syscall.PROT_READ
	}
	mmap, err = syscall.Mmap(int(map_file.Fd()), 0, int(file_size), mode, syscall.MAP_FILE|syscall.MAP_SHARED)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return mmap
}

func read_string(conn *net.Conn) (string, error) {
	len := int32(0)
	buf := make([]byte, 256, 256)
	binary.Read(*conn, binary.LittleEndian, &len)
	if len > 255 {
		log.Print("ERROR: string too long")
		os.Exit(1)
	}
	b := buf[:len]
	if _, err := io.ReadFull(*conn, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func HandleConn(conn net.Conn) {
	//input := make([]byte, RECV_BUF_LEN)
	//var buf []byte
	buf := make([]byte, 256, 256)
	defer conn.Close()
	for {
		e := Event{}

		// timestamp
		err := binary.Read(conn, binary.LittleEndian, &e.timestamp)
		if err != nil {
			log.Print("Error reading timestamp:", err.Error())
			return
		}
		// client_ip
		err = binary.Read(conn, binary.LittleEndian, &e.client_ip)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}
		e.event_type, err = read_string(&conn)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}
		e.user_agent, err = read_string(&conn)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}

		// country_code
		b := buf[:2]
		_, err = io.ReadFull(conn, b)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}
		copy(e.country_code[:], b)
		// region_code
		b = buf[:3]
		_, err = io.ReadFull(conn, b)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}
		copy(e.region_code[:], b)

		e.post_code, err = read_string(&conn)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}

		e.api_key, err = read_string(&conn)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}

		e.url, err = read_string(&conn)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}

		// user_uid
		e.user_uid, err = read_string(&conn)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}

		// user_quizzed
		b = buf[:1]
		_, err = io.ReadFull(conn, b)
		if err != nil {
			log.Print("Error reading:", err.Error())
			return
		}

		if b[0] == 0 {
			e.user_quizzed = false
		} else {
			e.user_quizzed = true
		}
		log.Printf("%+v\n", e)
		event_buffer = append(event_buffer, e)
		if len(event_buffer) == EVENT_BUFFER_LEN {
			write_column()
			event_buffer = make([]Event, 0, EVENT_BUFFER_LEN)
		}
	}
}

func write_column() {
	// typ := reflect.TypeOf(event_buffer[0])
	// for i := 0; i < typ.NumField(); i++ {
	// 	p := typ.Field(i)
	// }
}

func StartServer() {
	listen_addr := "0.0.0.0:4000"
	log.Print("Starting server on ", listen_addr)
	listener, err := net.Listen("tcp", listen_addr)
	if err != nil {
		println("error listening:", err.Error())
		os.Exit(1)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			println("Error accept:", err.Error())
			return
		}
		go HandleConn(conn)
	}
}

func main() {
	event_buffer = make([]Event, 0, EVENT_BUFFER_LEN)
	StartServer()
}
