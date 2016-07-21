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
	"time"

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
var patten string

/**
 * delimeter 기준으로 배열생성
 */
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

/**
 * 배열중복제거
 */
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

/**
 * 영문자 분리 (ex:컴퓨터PC => 컴퓨터 PC)
 */
func splitEng(word string) string {
	r := regexp.MustCompile("[a-zA-Z]+")
	words := strings.Fields(word)
	str := []string{}
	for _, w := range words {
		eng := r.FindString(w)
		pre := strings.TrimSpace(r.ReplaceAllString(w, ""))
		tail := strings.TrimSpace(eng)

		if pre != "" {
			str = append(str, pre)
		}

		if tail != "" {
			str = append(str, tail)
		}

	}
	str = removeDuplicates(str)

	return strings.Join(str, " ")
}

func analysis(customer_id string, mall_id string) {
	mall_array := []string{}
	if mall_id == "" {
		t_sql := "select mall_id from mall_category_info group by mall_id"
		rows, err := db.Query(t_sql)
		checkErr(err)
		defer rows.Close()

		for rows.Next() {
			var mall_id string
			rows.Scan(&mall_id)
			mall_array = append(mall_array, mall_id)
		}
	}

	if mall_id == "" {
		for _, mid := range mall_array {
			insertData(customer_id, mid)
		}
	} else {
		insertData(customer_id, mall_id)
	}

}

func insertData(customer_id string, mall_id string) {
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

		target_category := regSplit(category_name, patten)

		keywords := []string{}
		for _, dt := range target_category {
			keywords = append(keywords, strings.TrimSpace(dt))
		}
		keywords = removeDuplicates(keywords)
		keyword_str := strings.Join(keywords, " ")
		keyword_str = splitEng(keyword_str)

		_, err := db.Exec("INSERT IGNORE INTO category_customer_mall_match(customer_id,mall_id,cust_category_code,mall_category_code,register_date,update_date,rank,cust_category,mall_category) SELECT ?,?,?,category_code,NOW(),NOW(),match(category_nm) against(? IN BOOLEAN MODE) as score,?,category_nm from mall_category_info where mall_id=? and (match(category_nm) against(? IN BOOLEAN MODE))>0 ORDER BY (match(category_nm) against(? IN BOOLEAN MODE)) DESC LIMIT 5", customer_id, mall_id, category_id, keyword_str, category_name, mall_id, keyword_str, keyword_str)
		checkErr(err)
	}
}

func exec_cmd(cmd string) {
	out, err := exec.Command("sh", "-c", cmd).Output()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%s\n", out)

}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func init() {

	conf.Parse()
	if _, err := os.Stat("./database.conf"); os.IsNotExist(err) {
		log.Fatal(err)
	}
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", *dbuser, *dbpass, *dbhost, *dbport, *dbname)

	patten = "[()/^&*_,>-]+"
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var err error

	db, err = sql.Open("mysql", dsn)
	checkErr(err)
	defer db.Close()
	db.Query("SET NAMES utf8")

	if err = db.Ping(); err != nil {
		defer func() {
			fmt.Println(err)
		}()
		return
	}

	customer_id := flag.String("cid", "", "-cid=고객사아이디")
	mall_id := flag.String("mid", "", "-mid=몰아이디(옵션)")
	flag.Parse()

	if flag.NFlag() == 0 {
		flag.Usage()
		return
	}

	if *customer_id == "" {
		flag.Usage()
		return
	}

	ago := time.Now()

	analysis(*customer_id, *mall_id)

	now := time.Now()
	diff := now.Sub(ago)

	fmt.Printf("Done %v (Work Time : %v)\n", *customer_id, diff)

}
