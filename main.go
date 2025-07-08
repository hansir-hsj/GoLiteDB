package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("db > ")
		input, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		input = strings.TrimSpace(input)
		if input == ".exit" {
			break
		} else {
			fmt.Printf("Unrecognized command '%s'. \n", input)
		}
	}
}
