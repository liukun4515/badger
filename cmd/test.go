package main

import (
	"github.com/dgraph-io/badger/y"
	"github.com/juju/errors"
)

func main() {
	y.Check(errors.New("hello"))
}
