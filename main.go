package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255

	ID_SIZE         = 32
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + COLUMN_USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE

	PAGE_SIZE       = 4096
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

var (
	ErrTableFull           = fmt.Errorf("table is full")
	ErrPrepareSyntax       = fmt.Errorf("syntax error in statement")
	ErrPrepareUnRecognized = fmt.Errorf("unrecognized statement type")
)

type Row struct {
	ID       uint32
	Username [COLUMN_USERNAME_SIZE]byte
	Email    [COLUMN_EMAIL_SIZE]byte
}

type StatementType int

const (
	StatementTypeInsert StatementType = iota
	StatementTypeSelect
)

type Statement struct {
	Typ         StatementType
	RowToInsert Row
}

type Table struct {
	numRows uint32
	pages   [TABLE_MAX_PAGES]*[PAGE_SIZE]byte
}

type MetaCommandResult int

const (
	META_COMMAND_SUCCESS MetaCommandResult = iota
	META_COMMAND_UNRECOGNIZED
)

type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_SYNTAX_ERROR
	PREPARE_UNRECOGNIZED_STATEMENT
)

type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
)

func printRow(row *Row) {
	username := strings.TrimRight(string(row.Username[:]), "\x00")
	email := strings.TrimRight(string(row.Email[:]), "\x00")
	fmt.Printf("(%d, %s, %s)\n", row.ID, username, email)
}

// 序列化：将Row转成字节流
func serializeRow(src *Row, dest []byte) {
	binary.LittleEndian.PutUint32(dest[ID_OFFSET:ID_SIZE], src.ID)
	copy(dest[USERNAME_OFFSET:USERNAME_OFFSET+COLUMN_USERNAME_SIZE], src.Username[:])
	copy(dest[EMAIL_OFFSET:EMAIL_OFFSET+COLUMN_EMAIL_SIZE], src.Email[:])
}

// 反序列化：将字节流转成Row
func deserializeRow(src []byte, dest *Row) {
	dest.ID = binary.LittleEndian.Uint32(src[ID_OFFSET:ID_SIZE])
	copy(dest.Username[:], src[USERNAME_OFFSET:USERNAME_OFFSET+COLUMN_USERNAME_SIZE])
	copy(dest.Email[:], src[EMAIL_OFFSET:EMAIL_OFFSET+COLUMN_EMAIL_SIZE])
}

func NewTable() *Table {
	return &Table{
		numRows: 0,
		pages:   [TABLE_MAX_PAGES]*[PAGE_SIZE]byte{},
	}
}

func (t *Table) rowSlot(rowNum uint32) []byte {
	pageNum := rowNum / ROWS_PER_PAGE
	page := t.pages[pageNum]

	if page == nil {
		newPage := new([PAGE_SIZE]byte)
		t.pages[pageNum] = newPage
		page = newPage
	}

	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * uint32(ROW_SIZE)

	return page[byteOffset : byteOffset+ROW_SIZE]
}

func printPrompt() {
	fmt.Printf("db > ")
}

func doMetaCommand(input string) MetaCommandResult {
	if input == ".exit" {
		os.Exit(0)
	}
	return META_COMMAND_UNRECOGNIZED
}

func (stat *Statement) prepareStatement(input string) PrepareResult {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return PREPARE_UNRECOGNIZED_STATEMENT
	}

	switch parts[0] {
	case "insert":
		if len(parts) < 4 {
			return PREPARE_SYNTAX_ERROR
		}
		id, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return PREPARE_SYNTAX_ERROR
		}

		var username [COLUMN_USERNAME_SIZE]byte
		var email [COLUMN_EMAIL_SIZE]byte
		if len(parts[2]) > COLUMN_USERNAME_SIZE || len(parts[3]) > COLUMN_EMAIL_SIZE {
			return PREPARE_SYNTAX_ERROR
		}
		copy(username[:], parts[2])
		copy(email[:], parts[3])
		stat.Typ = StatementTypeInsert
		stat.RowToInsert = Row{
			ID:       uint32(id),
			Username: username,
			Email:    email,
		}

		return PREPARE_SUCCESS
	case "select":
		stat.Typ = StatementTypeSelect
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func (t *Table) executeInsert(stat *Statement) ExecuteResult {
	if t.numRows > TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	rowSlot := t.rowSlot(t.numRows)
	rowToInsert := &stat.RowToInsert

	serializeRow(rowToInsert, rowSlot)
	t.numRows++

	return EXECUTE_SUCCESS
}

func (t *Table) executeSelect() ExecuteResult {
	var row Row
	for i := uint32(0); i < t.numRows; i++ {
		rowSlot := t.rowSlot(i)
		deserializeRow(rowSlot, &row)
		printRow(&row)
	}
	return EXECUTE_SUCCESS
}

func (t *Table) executeStatement(stat *Statement) ExecuteResult {
	switch stat.Typ {
	case StatementTypeInsert:
		return t.executeInsert(stat)
	case StatementTypeSelect:
		return t.executeSelect()
	}
	return EXECUTE_SUCCESS
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	t := NewTable()

	for {
		printPrompt()
		input, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		input = strings.TrimSpace(input)

		if strings.HasPrefix(input, ".") {
			switch doMetaCommand(input) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED:
				fmt.Printf("Unrecognized command '%s'.\n", input)
				continue
			}
		}

		stat := &Statement{}
		switch stat.prepareStatement(input) {
		case PREPARE_SYNTAX_ERROR:
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", input)
			continue
		}

		switch t.executeStatement(stat) {
		case EXECUTE_SUCCESS:
			fmt.Println("Executed.")
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table full.")
		}

	}
}
