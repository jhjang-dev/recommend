package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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

	os.Mkdir("."+string(filepath.Separator)+"sql", 0777)
	file_nm := fmt.Sprintf("sql/%s.sql", customer_id)
	file, err := os.Create(file_nm)

	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer file.Close()

	t_sql := fmt.Sprintf("select full_category_name, full_category_id from category_customer_flatten_vw where customer_id='%s'", customer_id)
	t_rows, err := db.Query(t_sql)

	if err != nil {
		log.Fatal(err)
	}
	defer t_rows.Close()

	for t_rows.Next() {
		var category_name string
		var category_id string

		t_rows.Scan(&category_name, &category_id)

		target_category := regSplit(category_name, "[/^&*_,>]+")

		keywords := []string{}
		for _, dt := range target_category {
			keywords = append(keywords, strings.TrimSpace(dt))
		}

		var sql string
		if mall_id == "" {
			sql = "select mall_id ,category_code, category_nm from mall_category_info"
		} else {
			sql = fmt.Sprintf("select mall_id ,category_code, category_nm from mall_category_info where mall_id='%s'", mall_id)
		}

		rows, err := db.Query(sql)

		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var m_id string
			var c_code string
			var c_nm string

			rows.Scan(&m_id, &c_code, &c_nm)

			tmp_cate := regSplit(c_nm, "[/^&*_,>]+")
			tmp_cate = removeDuplicates(tmp_cate)
			comp_cate := strings.Join(tmp_cate, " ")

			var match_int int = 0
			for _, dt := range keywords {
				match, _ := regexp.MatchString(dt, comp_cate)

				if match {
					match_int++
				}
			}

			if match_int > 0 {

				if err != nil {
					defer func() {
						if er := recover(); er != nil {
							fmt.Println(er)
						}
					}()
				}

				t_sql := fmt.Sprintf("INSERT IGNORE INTO category_customer_mall_match(customer_id,mall_id,cust_category_code,mall_category_code,register_date,update_date,match_no) VALUES('%s','%s','%s','%s',NOW(),NOW(),'%d');\n", customer_id, m_id, category_id, c_code, match_int)
				file.WriteString(t_sql)

			}

		}


	}


}

func analysis2(customer_id string, mall_id string) {

	os.Mkdir("."+string(filepath.Separator)+"sql", 0777)
	file_nm := fmt.Sprintf("sql/%s.sql", customer_id)
	file, err := os.Create(file_nm)

	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer file.Close()

	t_sql := fmt.Sprintf("select full_category_name, full_category_id from category_customer_flatten_vw where customer_id='%s'", customer_id)
	t_rows, err := db.Query(t_sql)

	if err != nil {
		log.Fatal(err)
	}
	var seq int
	for t_rows.Next() {
		wg.Add(1)
		var category_name string
		var category_id string

		t_rows.Scan(&category_name, &category_id)

		target_category := regSplit(category_name, "[/^&*_,>]+")

		keywords := []string{}
		for _, dt := range target_category {
			keywords = append(keywords, strings.TrimSpace(dt))
		}

		go func(seq int, mid string) {
			defer wg.Done()

			fmt.Printf("Process Start Id : %v (%v)\n", seq, time.Now())
			var sql string
			if mall_id == "" {
				sql = "select mall_id ,category_code, category_nm from mall_category_info"
			} else {
				sql = fmt.Sprintf("select mall_id ,category_code, category_nm from mall_category_info where mall_id='%s'", mid)
			}

			rows, err := db.Query(sql)

			if err != nil {
				log.Fatal(err)
			}

			for rows.Next() {
				var m_id string
				var c_code string
				var c_nm string

				rows.Scan(&m_id, &c_code, &c_nm)

				tmp_cate := regSplit(c_nm, "[/^&*_,>]+")
				tmp_cate = removeDuplicates(tmp_cate)
				comp_cate := strings.Join(tmp_cate, " ")

				var match_int int = 0
				for _, dt := range keywords {
					match, _ := regexp.MatchString(dt, comp_cate)

					if match {
						match_int++
					}
				}

				if match_int > 0 {

					if err != nil {
						defer func() {
							if er := recover(); er != nil {
								fmt.Println(er)
							}
						}()
					}

					t_sql := fmt.Sprintf("INSERT IGNORE INTO category_customer_mall_match(customer_id,mall_id,cust_category_code,mall_category_code,register_date,update_date,match_no) VALUES('%s','%s','%s','%s',NOW(),NOW(),'%d');\n", customer_id, m_id, category_id, c_code, match_int)
					file.WriteString(t_sql)

				}

			}
			rows.Close()
			fmt.Printf("Process Stop Id : %v (%v)\n", seq, time.Now())
		}(seq, mall_id)

		if seq%100 == 0 && seq > 0 {
			wg.Wait()
		}
		seq++

	}
	t_rows.Close()
	wg.Wait()
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

	customer_id := flag.String("cid", "", "cid=고객사아이디")
	mall_id := flag.String("mid", "", "mid=몰아이디(옵션)")
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

	fmt.Println("DB Work Start ...")

	if *mall_id == "" {
		_, err = db.Query("delete from category_customer_mall_match where customer_id=?", customer_id)
	} else {
		_, err = db.Query("delete from category_customer_mall_match where customer_id=? and mall_id=?", customer_id, mall_id)
	}

	if err != nil {
		log.Fatal(err)
	}

	sql_file := fmt.Sprintf("./sql/%s.sql", *customer_id)
	_, err = os.Open(sql_file)
	if err != nil {
		fmt.Println(err)
	} else {
		cmd := fmt.Sprintf("mysql -h%s -u%s -p%s crawling_shoplinker < %s", *dbhost, *dbuser, *dbpass, sql_file)
		exec_cmd(cmd)
	}
}
