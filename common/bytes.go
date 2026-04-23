// Copyright © 2025 ANTDChain Contributors
// Licensed under the MIT License (MIT). See LICENSE in the repository root
// for more information.

package common

// LeftPadBytes pads a byte slice on the left with zeros to reach the specified length.
// If the input is longer than length, it is truncated from the left.
func LeftPadBytes(data []byte, length int) []byte {
    if len(data) >= length {
        return data[len(data)-length:]
    }
    padded := make([]byte, length)
    copy(padded[length-len(data):], data)
    return padded
}

// RightPadBytes pads a byte slice on the right with zeros to reach the specified length.
// If the input is longer than length, it is truncated from the right.
func RightPadBytes(data []byte, length int) []byte {
    if len(data) >= length {
        return data[:length]
    }
    padded := make([]byte, length)
    copy(padded, data)
    return padded
}
