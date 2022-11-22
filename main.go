package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"golang.org/x/exp/rand"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"golang.org/x/exp/constraints"
)

const size = 10_000_000

var (
	up       = *flag.String("upstream", "127.0.0.1:3306", "upstream")
	down     = *flag.String("downstream", "127.0.0.1:4000", "downstream")
	duration = *flag.Duration("time", time.Hour, "time")
)

func main() {
	flag.Parse()
	upstream := NewDB(fmt.Sprintf("root@tcp(%s)/test", up), func(n int64) bool {
		return n%2 == 0
	}, func() int64 {
		return int64(rand.Intn(size/2) * 2)
	})

	downstream := NewDB(fmt.Sprintf("root@tcp(%s)/test", down), func(n int64) bool {
		return n%2 == 1
	}, func() int64 {
		return int64(rand.Intn(size/2)*2) + 1
	})

	log.Println("prepare data")
	start := time.Now()
	uc, dc := make(chan struct{}), make(chan struct{})

	go upstream.prepare(uc)
	go downstream.prepare(dc)

	<-uc
	<-dc

	log.Println("prepare data done, take time", time.Since(start))

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	log.Println("start dml")

	go upstream.run(ctx, uc)
	go downstream.run(ctx, dc)

	<-uc
	<-dc
}

type DB struct {
	db       *sql.DB
	filter   func(n int64) bool
	producer func() int64
}

func NewDB(uri string, filter func(n int64) bool, producer func() int64) *DB {
	db, err := sql.Open("mysql", uri)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1024; i++ {
		if !filter(producer()) {
			log.Fatal("producer and filter is not match")
		}
	}
	return &DB{db: db, filter: filter, producer: producer}
}

func (d *DB) init() {
	_, err := d.db.Exec("drop table if exists t;")
	if err != nil {
		log.Fatal(err)
	}
	_, err = d.db.Exec("create table t(a int primary key, b char(64));")
	if err != nil {
		log.Fatal(err)
	}
}

func (d *DB) prepare(done chan struct{}) {
	var wg sync.WaitGroup

	d.init()

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			values := make([]string, 0, 1000)
			start := idx * ((size + 8) / 8)
			end := min((idx+1)*((size+8)/8), size)
			for pk := start; pk < end; pk++ {
				if len(values) >= 1000 || pk == end-1 {
					query := "insert into t values " + strings.Join(values, ",")
					_, err := d.db.Exec(query)
					if err != nil {
						log.Fatal(err, query)
					}
					start = pk
					values = values[:0]
				}
				if !d.filter(int64(pk)) || rand.Intn(2) == 0 {
					continue
				}
				values = append(values, fmt.Sprintf("(%d, '%s')", pk, uuid.New().String()))
			}
		}(i)
	}

	wg.Wait()
	done <- struct{}{}
}

func (d *DB) run(ctx context.Context, done chan struct{}) {
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		conn, err := d.db.Conn(ctx)
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func(c *sql.Conn) {
			d.runRandomDMLInTxn(ctx, c)
			_ = c.Close()
			wg.Done()
		}(conn)
	}
	wg.Wait()
	done <- struct{}{}
}

func (d *DB) runRandomDMLInTxn(ctx context.Context, conn *sql.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				log.Println("begin failed", err)
				time.Sleep(3 * time.Second)
				continue
			}
			for i := 0; i < rand.Intn(128)+rand.Intn(128)+rand.Intn(128); i++ {
				x := rand.Intn(9)
				switch {
				case x < 3:
					tx.Exec("delete from t where a = ?", d.producer())
				case 3 <= x && x < 6:
					if rand.Intn(5) == 0 {
						tx.Exec("update t set a = ? where a = ?", d.producer(), d.producer())
					} else {
						tx.Exec("update t set b = ? where a = ?", uuid.New().String(), d.producer())
					}
				case 6 <= x && x < 9:
					tx.Exec("replace into t values (?, ?)", d.producer(), uuid.New().String())
				}
			}
			if rand.Intn(10) == 0 {
				err = tx.Rollback()
				if err != nil {
					log.Println("rollback failed", err)
					time.Sleep(3 * time.Second)
				}
			} else {
				err = tx.Commit()
				if err != nil {
					log.Println("commit failed", err)
					time.Sleep(3 * time.Second)
				}
			}

		}
	}
}

func min[T constraints.Ordered](x T, xs ...T) T {
	m := x
	for _, n := range xs {
		if n < m {
			m = n
		}
	}
	return m
}
