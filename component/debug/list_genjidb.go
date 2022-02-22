package debug

import "github.com/genjidb/genji"

func ListDocDBData(db *genji.DB) {
	db.Query("show tables")
}
