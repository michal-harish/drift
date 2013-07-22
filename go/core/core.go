package core

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
	RECV_BUF_LEN = 200
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
	user_uid     [16]byte
	user_quizzed bool
}

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

func read_string(conn *net.Conn) string {
	len := int32(0)
	buf := make([]byte, 256, 256)
	binary.Read(*conn, binary.LittleEndian, &len)
	b := buf[:len]
	if _, err := io.ReadFull(*conn, b); err != nil {
		return ""
	}
	return string(b)
}

func HandleConn(conn net.Conn) {
	//input := make([]byte, RECV_BUF_LEN)
	//var buf []byte
	buf := make([]byte, 256, 256)
	defer conn.Close()
	for {
		// TODO: error checking!
		e := Event{}
		// timestamp
		binary.Read(conn, binary.LittleEndian, &e.timestamp)
		// client_ip
		binary.Read(conn, binary.LittleEndian, &e.client_ip)
		e.event_type = read_string(&conn)
		e.user_agent = read_string(&conn)
		// country_code
		b := buf[:2]
		io.ReadFull(conn, b)
		copy(e.country_code[:], b)
		// region_code
		b = buf[:3]
		io.ReadFull(conn, b)
		copy(e.region_code[:], b)
		e.post_code = read_string(&conn)
		e.api_key = read_string(&conn)
		e.url = read_string(&conn)
		// user_uid
		b = buf[:16]
		io.ReadFull(conn, b)
		copy(e.user_uid[:], b)
		// user_quizzed
		b = buf[:1]
		io.ReadFull(conn, b)
		if b[1] == 0 {
			e.user_quizzed = false
		} else {
			e.user_quizzed = true
		}
		log.Printf("%+v\n", e)
	}
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
	StartServer()
}
