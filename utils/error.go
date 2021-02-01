package utils

import (
	log "github.com/sirupsen/logrus"
)

func CheckErrFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
