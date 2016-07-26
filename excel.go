package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/c4pt0r/ini"
	_ "github.com/go-sql-driver/mysql"
	"github.com/tealeg/xlsx"
)

var conf = ini.NewConf("database.conf")
var (
	dbhost = conf.String("database", "dbhost", "")
	dbname = conf.String("database", "dbname", "")
	dbuser = conf.String("database", "dbuser", "")
	dbpass = conf.String("database", "dbpass", "")
	dbport = conf.String("database", "dbport", "")
)

var db *sql.DB
var dsn string

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

}

func main() {
	var file *xlsx.File
	var sheet *xlsx.Sheet
	var row *xlsx.Row
	var cell *xlsx.Cell
	var err error

	c_id := flag.String("cid", "", "-cid=고객사아이디")
	m_id := flag.String("mid", "", "-mid=몰아이디")
	flag.Parse()

	if flag.NFlag() < 1 {
		flag.Usage()
		return
	}

	if *c_id == "" || *m_id == "" {
		flag.Usage()
		return
	}

	customer_id := fmt.Sprintf("%v", *c_id)
	mall_id := fmt.Sprintf("%v", *m_id)

	style := xlsx.NewStyle()

	fill := *xlsx.NewFill("solid", "D8D8D8", "D8D8D8")
	font := *xlsx.NewFont(15, "Verdana")
	border := *xlsx.NewBorder("thin", "thin", "thin", "thin")
	align := *xlsx.DefaultAlignment()
	align.Horizontal = "center"

	style.Fill = fill
	style.Font = font
	style.Border = border
	style.Alignment = align

	style.ApplyFill = true
	style.ApplyFont = true
	style.ApplyBorder = true

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

	file = xlsx.NewFile()
	sheet, err = file.AddSheet("Sheet1")
	checkErr(err)

	sql := "select a.full_category_name,c.category_nm,b.rank from (select customer_id,full_category_name,full_category_id from category_customer_flatten_vw where customer_id='" + customer_id + "') a left join category_customer_mall_match b on a.full_category_id = b.cust_category_code left join mall_category_info c on (b.mall_category_code=c.category_code) where a.customer_id='" + customer_id + "' and b.mall_id='" + mall_id + "' order by rank desc;"

	rows, err := db.Query(sql)
	checkErr(err)
	defer rows.Close()

	row = sheet.AddRow()
	cell = row.AddCell()
	cell.Value = "고객사 카테고리"
	cell.SetStyle(style)

	cell = row.AddCell()
	cell.Value = "몰 카테고리"
	cell.SetStyle(style)

	cell = row.AddCell()
	cell.Value = "Rank"
	cell.SetStyle(style)

	for rows.Next() {
		var cust_category_nm string
		var mall_category_nm string
		var rank string

		rows.Scan(&cust_category_nm, &mall_category_nm, &rank)
		row = sheet.AddRow()
		cell = row.AddCell()
		cell.Value = cust_category_nm

		cell = row.AddCell()
		cell.Value = mall_category_nm

		cell = row.AddCell()
		cell.Value = rank
	}

	sheet.SetColWidth(0, 1, 50)
	sheet.SetColWidth(2, 2, 20)

	f_nm := fmt.Sprintf("%v-%v.xlsx", customer_id, mall_id)
	err = file.Save(f_nm)
	checkErr(err)

}
