//
// Test suite for engine2.go
//

package engine2

import (
	"fmt"
	"testing"
)

func TestRegister(t *testing.T) {

	var uuid string

	db, err := GetDatabase("rainer")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	uuid, err = db.RegisterLocalNode("test.towerpower.co", "rainer/nodes")
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("Register to %v\n", uuid)

}

func TestNewTSN(t *testing.T) {

	var tsn int64

	db, err := GetDatabase("rainer")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	tsn, err = db.NewNodesTSN()
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("1 x NEW TSN from engine2 %v\n", tsn)

}

func TestGetRemoteHighs(t *testing.T) {
	var hwms HighWaterMarks

	db, err := GetDatabase("rainer")

	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	hwms, err = db.GetRemoteHighs()
	if err != nil {
		fmt.Printf("PANIC %#v\n", err)
		t.FailNow()
	}

	fmt.Printf("GetRemoteHighs Len %v Cap %v\n%v\n", len(hwms), cap(hwms), hwms)

}
