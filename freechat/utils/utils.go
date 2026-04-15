package utils

import (
	gonanoid "github.com/matoous/go-nanoid/v2"
)

func BuildCredentialPhone(areaCode, phone string) string {
	return areaCode + " " + phone
}

func NewId() (string, error) {
	alphabet := "0123456789"
	id, err := gonanoid.Generate(alphabet, 20)
	return id, err
}
