package main

import "fmt"

func main() {
	b := &batch{}
	b.add([]byte("hello"))
	fmt.Println([]byte("hello"))
	fmt.Println(b.keys)

	var key []string
	key = append(key, "hello")
	fmt.Println(key)
}

type batch struct {
	keys   [][]byte
	values [][]byte
}

func (b *batch) add(key []byte) {
	b.keys = append(b.keys, key)
}
