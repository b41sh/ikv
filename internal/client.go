package internal

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

type Client struct {
}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) Run() {

	port := "127.0.0.1:6379"
	conn, err := net.Dial("tcp", port)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print(">>> ")
		cmd, _ := reader.ReadString('\n')
		if strings.TrimSpace(string(cmd)) == "" {
			continue
		}
		if strings.TrimSpace(string(cmd)) == "exit" {
			fmt.Println("client exiting...")
			return
		}
		fmt.Fprintf(conn, cmd+"\n")
		resp, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print(resp)
	}
}
