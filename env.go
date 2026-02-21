package main

import "os"

// Getenv returns enviroment for a key or a default value
func Getenv(key, def string) string {
	env := os.Getenv(key)
	if env == "" {
		env = def
	}

	return env
}
