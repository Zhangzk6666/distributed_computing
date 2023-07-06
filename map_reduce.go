package distributed_computing

// 定义map和reduce方法

import (
	"distributed_computing/mr"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// 将信息转成kv
func Map(filename string, contents string) []mr.KeyValue {
	kva := []mr.KeyValue{}
	ff := func(r rune) bool { return r == []rune("\n")[0] }
	words := strings.FieldsFunc(contents, ff)
	for i := 0; i < len(words); i++ {
		accountMsg := strings.Split(words[i], " ")
		fmt.Println("getOneMsg....", accountMsg)
		if len(accountMsg) > 3 {
			money, err := decimal.NewFromString(accountMsg[1])
			if err != nil {
				fmt.Println(err.Error())
			}
			stringTime := strings.Trim(fmt.Sprint(accountMsg[2:]), "[]")
			operateTime, err := time.ParseInLocation("2006-01-02 15:04:05", stringTime, time.Local)
			if err != nil {
				fmt.Println(err.Error())
			}
			kv := mr.KeyValue{
				Key: mr.KeyMsg(accountMsg[0]),
				Value: mr.ValueMsg{
					Money:       money,
					OperateTime: operateTime,
				}}
			kva = append(kva, kv)
		}
	}
	return kva
}

/**
Reduce 方法可以做各种统计,如:
1、统计每个账号全部时间内的总金额
2、统计每个账号同一天（0-24小时）账号的总金额
3、统计每个账号指定时间内的总金额

当前以 `统计全每个账号部时间内的总金额` 为例子
**/

// 统计每个账号全部时间内的总金额
func Reduce(key mr.KeyMsg, values []mr.ValueMsg) string {
	var totalMoney decimal.Decimal
	for i := 0; i < len(values); i++ {
		totalMoney = totalMoney.Add(values[i].Money)
	}
	return totalMoney.String()
}
