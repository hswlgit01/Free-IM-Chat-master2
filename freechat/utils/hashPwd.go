package utils

import (
	"golang.org/x/crypto/bcrypt"
)

func HashPassword(pwd string) (string, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(pwd), bcrypt.DefaultCost)
	return string(hashedPassword), err
}

func CheckPassword(providedPwd string, storedPwd string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(storedPwd), []byte(providedPwd))
	return err == nil
}
