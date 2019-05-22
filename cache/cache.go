package cache

import (
	"errors"
)

var (
	// ErrExpired is returned when the key-value tuple has expired.
	ErrExpired = errors.New("expired")
)
