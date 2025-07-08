package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type StatementType int

const (
	StatementTypeInsert StatementType = iota
	StatementTypeSelect
)

type Statement struct {
	Typ StatementType
}

func main() {
	reader := bufio.NewReader(os.Stdin)

outer:
	for {
		fmt.Print("db > ")
		input, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		input = strings.TrimSpace(input)

		if strings.HasPrefix(input, ".") {
			switch input {
			case ".exit":
				break outer
			default:
				fmt.Println("Unrecognized command:", input)
				continue
			}
		}

		var stat Statement
		if strings.HasPrefix(input, "insert") {
			stat.Typ = StatementTypeInsert
		} else if strings.HasPrefix(input, "select") {
			stat.Typ = StatementTypeSelect
		} else {
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue
		}

		switch stat.Typ {
		case StatementTypeInsert:
			fmt.Println("This is where we would do an insert.")
		case StatementTypeSelect:
			fmt.Println("This is where we would do a select.")
		}

		fmt.Println("Executed.")
	}
}
