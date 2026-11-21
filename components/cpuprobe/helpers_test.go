// SPDX-License-Identifier: Apache-2.0

package main

import "fmt"

func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }
