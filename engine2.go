// ENGINE NODES
//
// Package for manage power engine data
//
// The package offers RESTful functions to store and retrieve objects
// Objects are versioned using a sequence number generator
//
package engine2

import (
	"database/sql"
	"errors"
	//"fmt"
	_ "github.com/lib/pq"
	//"log"
	//	"os"
	//	"strings"
	//	"sync"
)

type HighWaterMark struct {
	nodeid  string
	clockid int64
	tsn     int64
}

type HighWaterMarks []HighWaterMark

/* read sql Rows into HighWaterMarks structure
 *
 * the assumed position in the rows is
 * $1  nodeid
 * $2  cockid
 * $3  tsn
 */
func rowsToHighWaterMarks(rows sql.Rows) HighWaterMarks {
	var (
		hwm  HighWaterMark
		hwms HighWaterMarks
		err  error
	)

	for rows.Next() {
		err = rows.Scan(&hwm.nodeid, &hwm.clockid, &hwm.tsn)
		checkErr("scan high water mark", err)
	}
	err = rows.Err()
	checkErr("end loop", err)
	/**/

	return hwms
}

/*
// helper function for error handling (go panic!)
func checkErr(trace string, err error) {

	if err != nil {
		fmt.Printf("ERROR: %#v\n", err)
		log.Panic(err)
	}

}

// helper function for tracing a SQL return row
// some better idea needed eventually (->tracing)
func checkRow(row *sql.Row) {

	// fmt.Printf( "ROW: %#v\n", row )

}
*/

// Calling database stored functions

// Retrieve a new TSN from database as int64
func newTSN2(dbconnect *sql.DB) int64 {

	var tsn int64

	row := dbconnect.QueryRow("select nodes.new_tsn()")
	checkRow(row)

	err := row.Scan(&tsn)
	checkErr("nodes: newTSN", err)

	return tsn
}

// Register node
func registerLocalNode(dbconnect *sql.DB, in_url string, in_data string) string {

	var out_id string

	row := dbconnect.QueryRow("select nodes.register( $1, $2 )", in_url, in_data)
	checkRow(row)

	err := row.Scan(&out_id)

	checkErr("nodes.register", err)

	return out_id
}

func getRemoteHighs(dbconnect *sql.DB) HighWaterMarks {
	var hwms HighWaterMarks

	rows, err := dbconnect.Query("select nodes.getRemoteHighs()")
	defer rows.Close()

	checkErr("getRemoteHighs", err)
	// etab = EngineTable.FromRows( rows )`

	return hwms
}

//
// PACKAGE EXPORTS

// From a given database object retrieve the next TSN
//
// Package Export
func (db *Database) NewNodesTSN() (tsn int64, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while reading nodes TSN")

		}

	}()

	tsn = newTSN(db.dbconnect)
	return
}

// Intial Registration
//
// Package Export
func (db *Database) RegisterLocalNode(in_url string, in_data string) (out_value string, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while getting power data")

		}

	}()

	out_value = registerLocalNode(db.dbconnect, in_url, in_data)

	return out_value, err
}

/*
// Put a new value
func putPowerData(dbconnect *sql.DB, in_key string, in_value string) {

	_, err := dbconnect.Exec("select power.put( $1, $2 )", in_key, in_value)

	checkErr("power.put", err)

	return
}

// get a value
func getPowerData(dbconnect *sql.DB, in_key string) string {

	var out_value string

	row := dbconnect.QueryRow("select power.get( $1 )", in_key)
	checkRow(row)

	err := row.Scan(&out_value)
	checkErr("getPowerData", err)

	return out_value
}

//
// PACKAGE EXPORTS


// Put power.data
//
// Package Export
func (db *Database) PutPowerData(in_key string, in_value string) (err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while inserting power data")

		}

	}()

	putPowerData(db.dbconnect, in_key, in_value)
	return
}

// Get Power.data
//
// Package Export
func (db *Database) GetPowerData(in_key string) (out_value string, err error) {

	defer func() {

		if r := recover(); r != nil {
			// recover from panic
			err = errors.New("error while getting power data")

		}

	}()

	out_value = getPowerData(db.dbconnect, in_key)

	return out_value, err
}
*/
