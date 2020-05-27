package internal

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"
)

const (
	cmdGet            = "get"
	cmdGetLen         = 2
	errEmptyCmd       = "(error) ERR unknown command\n"
	errUnknownCmd     = "(error) ERR unknown command '%s'\n"
	errWrongNumberCmd = "(error) ERR wrong number of arguments for '%s' command\n"
	errKeyNotExist    = "(nil)\n"
)

type Server struct {
	db *Db
}

func NewServer() (*Server, error) {
	db, err := NewDb()
	if err != nil {
		return &Server{}, err
	}
	db.Init()

	return &Server{
		db: db,
	}, nil
}

func (s *Server) Run() {
	fmt.Println("run server")

	port := ":6379"
	l, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()
	rand.Seed(time.Now().Unix())

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go s.handler(c)
	}
}

func (s *Server) handler(conn net.Conn) {
	fmt.Printf("Serving %s\n", conn.RemoteAddr().String())
	for {
		data, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			return
		}
		data = strings.TrimSpace(data)
		cmds := strings.Split(data, " ")

		if len(cmds) == 0 {
			conn.Write([]byte(errEmptyCmd))
			continue
		}
		cmd := strings.ToLower(cmds[0])
		if cmd != cmdGet {
			conn.Write([]byte(fmt.Sprintf(errUnknownCmd, cmd)))
			continue
		}
		if len(cmds) != cmdGetLen {
			conn.Write([]byte(fmt.Sprintf(errWrongNumberCmd, cmd)))
			continue
		}
		key := cmds[1]
		value, err := s.db.Get(key)
		if err != nil {
			conn.Write([]byte(errKeyNotExist))
			continue
		}
		conn.Write(value)
		conn.Write([]byte{10})
	}
	conn.Close()
}
