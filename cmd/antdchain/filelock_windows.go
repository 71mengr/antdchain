// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

//go:build windows

package main

import (
	"errors"
	"os"
)

var errDataDirLocked = errors.New("data directory is already locked")

func lockFileExclusiveNonBlocking(file *os.File) error {
	_ = file
	return nil
}

func unlockFile(file *os.File) error {
	_ = file
	return nil
}

