package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"github.com/c4pt0r/ini"
	_ "github.com/go-sql-driver/mysql"
)

var conf = ini.NewConf("database.conf")
var (
	dbhost = conf.String("database", "dbhost", "")
	dbname = conf.String("database", "dbname", "")
	dbuser = conf.String("database", "dbuser", "")
	dbpass = conf.String("database", "dbpass", "")
	dbport = conf.String("database", "dbport", "")
)

var wg sync.WaitGroup
var db *sql.DB
var dsn string

func regSplit(text string, delimeter string) []string {
	reg := regexp.MustCompile(delimeter)
	indexes := reg.FindAllStringIndex(text, -1)
	laststart := 0

	result := make([]string, len(indexes)+1)

	for i, element := range indexes {
		result[i] = text[laststart:element[0]]
		laststart = element[1]
	}
	result[len(indexes)] = text[laststart:len(text)]

	return result
}

func removeDuplicates(elements []string) []string {
	encountered := map[string]bool{}

	// Create a map of all unique elements.
	for v := range elements {
		encountered[elements[v]] = true
	}

	// Place all keys from the map into a slice.
	result := []string{}
	for key, _ := range encountered {
		result = append(result, key)
	}
	return result
}

func analysis(customer_id string, mall_id string) {

	sql := fmt.Sprintf("select full_category_name, full_category_id from category_customer_flatten_vw where customer_id='%s'", customer_id)
	rows, err := db.Query(sql)
	defer rows.Close()

	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var category_name string
		var category_id string

		rows.Scan(&category_name, &category_id)

		target_category := regSplit(category_name, "[()/^&*_,>]+")

		keywords := []string{}
		for _, dt := range target_category {
			keywords = append(keywords, strings.TrimSpace(dt))
		}
		keyword_str := strings.Join(keywords, " ")

		var t_sql string

		if mall_id == "" {
			t_sql = fmt.Sprintf("INSERT IGNORE INTO category_customer_mall_match(customer_id,mall_id,cust_category_code,mall_category_code,register_date,update_date,rank) SELECT '%s',mall_id,'%s',category_code,NOW(),NOW(),match(category_nm) against('%s' IN BOOLEAN MODE) as score from mall_category_info where match(category_nm) against('%s' IN BOOLEAN MODE)  > 0", customer_id, category_id, keyword_str, keyword_str)
		} else {
			t_sql = fmt.Sprintf("INSERT IGNORE INTO category_customer_mall_match(customer_id,mall_id,cust_category_code,mall_category_code,register_date,update_date,rank) SELECT '%s','%s','%s',category_code,NOW(),NOW(),match(category_nm) against('%s' IN BOOLEAN MODE) as score from mall_category_info where mall_id='%s' and match(category_nm) against('%s' IN BOOLEAN MODE)  > 0", customer_id, mall_id, category_id, keyword_str, mall_id, keyword_str)
		}

		_, err := db.Exec(t_sql)
		if err != nil {
			log.Fatal(err)
		}

	}

}

func exec_cmd(cmd string) {
	out, err := exec.Command("sh", "-c", cmd).Output()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%s\n", out)

}

func init() {

	conf.Parse()
	if _, err := os.Stat("./database.conf"); os.IsNotExist(err) {
		log.Fatal(err)
	}
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", *dbuser, *dbpass, *dbhost, *dbport, *dbname)

}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var err error

	customer_id := flag.String("cid", "", "-cid=고객사아이디")
	mall_id := flag.String("mid", "", "-mid=몰아이디(옵션)")
	flag.Parse()

	if flag.NFlag() == 0 {
		flag.Usage()
		return
	}

	db, err = sql.Open("mysql", dsn)

	if err != nil {
		log.Fatal(err)
	}
	db.Query("SET NAMES utf8")

	if err = db.Ping(); err != nil {
		defer func() {
			fmt.Println(err)
		}()
		return
	}
	defer db.Close()

	analysis(*customer_id, *mall_id)

}
