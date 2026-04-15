package utils

import (
	"os"
	"strconv"
)

func IsLocalTestEnv() bool {
	envVar := os.Getenv("IS_LOCAL_TEST")
	result, _ := strconv.ParseBool(envVar)
	return result
}
