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
		keywords = removeDuplicates(keywords)
		keyword_str := strings.Join(keywords, " ")
		keyword_str = splitEng(keyword_str)

		if mall_id == "" {
			_, err := db.Exec("INSERT IGNORE INTO category_customer_mall_match(customer_id,mall_id,cust_category_code,mall_category_code,register_date,update_date,rank) SELECT ?,mall_id,?,category_code,NOW(),NOW(),match(category_nm) against(? IN BOOLEAN MODE) as score from mall_category_info where (match(category_nm) against(? IN BOOLEAN MODE))>0 ORDER BY (match(category_nm) against(? IN BOOLEAN MODE)) DESC LIMIT 5", customer_id, category_id, keyword_str, keyword_str, keyword_str)
			checkErr(err)
		} else {
			_, err := db.Exec("INSERT IGNORE INTO category_customer_mall_match(customer_id,mall_id,cust_category_code,mall_category_code,register_date,update_date,rank) SELECT ?,?,?,category_code,NOW(),NOW(),match(category_nm) against(? IN BOOLEAN MODE) as score from mall_category_info where mall_id=? and (match(category_nm) against(? IN BOOLEAN MODE))>0 ORDER BY (match(category_nm) against(? IN BOOLEAN MODE)) DESC LIMIT 5", customer_id, mall_id, category_id, keyword_str, mall_id, keyword_str, keyword_str)
			checkErr(err)
		}

	}

}

func matchProc(id int, customer_id string) {

	fmt.Printf("Process Start Id : %v-%v (%v)\n", id, customer_id, time.Now())

	var t_sql string
	t_sql = fmt.Sprintf("select full_category_name, full_category_id from category_customer_flatten_vw where customer_id='%s'", customer_id)
	t_rows, err := db.Query(t_sql)
	defer t_rows.Close()
	checkErr(err)

	for t_rows.Next() {

		var category_name string
		var category_id string

		t_rows.Scan(&category_name, &category_id)

		target_category := regSplit(category_name, "[()/^&*_,>]+")

		keywords := []string{}
		for _, dt := range target_category {
			keywords = append(keywords, strings.TrimSpace(dt))
		}
		keywords = removeDuplicates(keywords)
		keyword_str := strings.Join(keywords, " ")
		keyword_str = splitEng(keyword_str)

		_, er := db.Exec("INSERT IGNORE INTO category_customer_mall_match(customer_id,mall_id,cust_category_code,mall_category_code,register_date,update_date,rank) SELECT ?,mall_id,?,category_code,NOW(),NOW(),match(category_nm) against(? IN BOOLEAN MODE) as score from mall_category_info where (match(category_nm) against(? IN BOOLEAN MODE)) >0 ORDER BY (match(category_nm) against(? IN BOOLEAN MODE)) DESC LIMIT 5", customer_id, category_id, keyword_str, keyword_str, keyword_str)
		checkErr(er)
	}
	fmt.Printf("Process Stop Id : %v-%v (%v)\n", id, customer_id, time.Now())
}

func analysis_routine() {

	sql := "select customer_id,count(*) as cnt from category_customer where length(customer_id)>0 group by customer_id order by cnt asc"
	rows, err := db.Query(sql)
	defer rows.Close()

	if err != nil {
		log.Fatal(err)
	}

	var i int
	for rows.Next() {
		var customer_id string
		var cnt int
		rows.Scan(&customer_id, &cnt)

		matchProc(i, customer_id)
		i++

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

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var err error

	db, err = sql.Open("mysql", dsn)
	checkErr(err)
	defer db.Close()

	db.Query("SET NAMES utf8")

	args := os.Args
	customer_id := flag.String("cid", "", "-cid=고객사아이디")
	mall_id := flag.String("mid", "", "-mid=몰아이디(옵션)")
	flag.Parse()

	if len(args) > 1 {

		if flag.NFlag() == 0 {
			flag.Usage()
			return
		}
		analysis(*customer_id, *mall_id)
	} else {

		analysis_routine()
	}

}
