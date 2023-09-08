package goxstreams

import (
	"log"
	"os"
)

type logger struct {
	log *log.Logger
}

func newLogger() logger {
	return logger{log.New(os.Stderr, "ERROR\t", log.Ldate|log.Ltime)}
}

func (l logger) err(err error) {
	l.log.Println(err)
}

func (l logger) print(str string) {
	l.log.Println(str)
}

func (l logger) fatal(str string) {
	l.log.Fatalln(str)
}
