package main

import (
	"github.com/testground/sdk-go/run"
)

var testcases = map[string]interface{}{
	"test": run.InitializedTestCaseFn(test),
}

func main() {
	run.InvokeMap(testcases)
}
