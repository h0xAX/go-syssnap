package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type mysqlpr struct {
	User     sql.NullString
	Host     sql.NullString
	Db       sql.NullString
	Command  sql.NullString
	Time     sql.NullString
	State    sql.NullString
	Info     sql.NullString
	Progress sql.NullString
}
type mysqlWatcher struct {
	Timer       int64
	StartTime   int64
	EndTime     int64
	Processlist map[string]*mysqlpr
}

func newMysqlWatcher(t int64) *mysqlWatcher {
	return &mysqlWatcher{
		Timer: t,
	}
}

func (m *mysqlWatcher) start(queuer chan map[string]interface{}) {
	for {
		m.StartTime = time.Now().Unix()
		db, err := sql.Open("mysql", "root:@/?charset=utf8")
		checkErr(err)

		m.Processlist = make(map[string]*mysqlpr)

		rows, err := db.Query("SHOW FULL PROCESSLIST")
		checkErr(err)

		for rows.Next() {
			var id string
			pr := &mysqlpr{}
			err = rows.Scan(&id, &pr.User, &pr.Host, &pr.Db, &pr.Command, &pr.Time, &pr.State, &pr.Info, &pr.Progress)
			checkErr(err)
			m.Processlist[id] = pr
		}
		db.Close()

		queuer <- map[string]interface{}{
			"processlist": m.Processlist,
		}
		m.EndTime = time.Now().Unix()

		// Try to sync timings, but don't overload.
		s := m.Timer - (m.EndTime - m.StartTime)
		if s < m.Timer/2 {
			s = m.Timer
		}
		time.Sleep(time.Duration(s) * time.Second)
	}

}

func main() {
	done := make(chan bool)
	queuer := make(chan map[string]interface{}, 50)
	mWatch := newMysqlWatcher(3)
	go mWatch.start(queuer)
	go writeToLog(queuer)
	<-done
}

func checkErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func writeToLog(queuer chan map[string]interface{}) {
	logFile, err := os.OpenFile("sys-snap.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	checkErr(err)
	defer logFile.Close()
	writer := bufio.NewWriter(logFile)

	for msg := range queuer {
		data, err := json.Marshal(msg)
		checkErr(err)
		fmt.Println(string(data))
		checkErr(err)
		data = append(data, byte('\n'))
		_, err = writer.Write(data)
		writer.Flush()
		checkErr(err)
	}

}

type pParser struct {
	SystemStarttime int
	Processes       map[string]string
	Sockets         map[string]string
	Sysinfo         map[string]string
}

func (p *pParser) getSystemUptime() float64 {
	file, err := os.Open("/proc/uptime")
	checkErr(err)
	defer file.Close()

	rdr := bufio.NewReader(file)
	load, err := rdr.ReadString(' ')
	checkErr(err)
	load = strings.TrimSpace(load)
	if load, err = strconv.ParseFloat(load, 64); err != nil {
		return load
	}
	return 0.0
}
