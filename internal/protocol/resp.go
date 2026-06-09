package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)


func ReadCommand(r *bufio.Reader) ([]string, error) {	
	b, err := r.Peek(1)
	if err != nil {
		return nil, err
	}

	if b[0] != '*' {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			return nil, errors.New("empty command")
		}
		return parts, nil
	}
	
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	arrLen, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}

	args := make([]string, arrLen)
	for i := 0; i < arrLen; i++ {
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		
		if line[0] != '$' {
			return nil, errors.New("expected RESP bulk string")
		}

		strLen, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		
		buf := make([]byte, strLen+2) 
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return nil, err
		}
		args[i] = string(buf[:strLen])
	}
	return args, nil
}

func WriteSimpleString(w io.Writer, s string) {
	fmt.Fprintf(w, "+%s\r\n", s) 
}

func WriteError(w io.Writer, msg string) {
	fmt.Fprintf(w, "-ERR %s\r\n", msg) 
}

func WriteRedirect(w io.Writer, node string, key string) {	
	fmt.Fprintf(w, "-MOVED %s %s\r\n", key, node)
}

func WriteBulkString(w io.Writer, s string) {
	fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s) 
}

func WriteNull(w io.Writer) {
	w.Write([]byte("$-1\r\n")) 
}