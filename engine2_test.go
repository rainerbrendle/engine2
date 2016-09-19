//
// Test suite for engine2.go
//

package engine2

import (
	"engine"
	"fmt"
	"testing"
)

func TestNewTSN(t *testing.T) {

	var tsn int64

	db, err := GetDatabase("rainer")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	tsn, err = db.NewTSN2()
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("1 x NEW TSN from engine2 %v\n", tsn)

}
