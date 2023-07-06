package main

import (
	"distributed_computing"
	"distributed_computing/mr"
	"runtime"
	"time"
)

var (
	askJobTime time.Duration = 30 * time.Second
	nReduce    int           = 4
)

/*
*
账户 1 10.38 2023-05-31 14:00:22
账户 2 20.99 2023-06-05 09:59:33
账户 1 34.88 2023-05-31 15:41:22
*
*/
func main() {
	files := make([]string, 0)
	files = append(files, "doc/account-msg.txt")
	files = append(files, "doc/account-msg1.txt")
	files = append(files, "doc/account-msg2.txt")
	files = append(files, "doc/account-msg3.txt")
	files = append(files, "doc/account-msg4.txt")
	files = append(files, "doc/account-msg5.txt")
	files = append(files, "doc/account-msg6.txt")
	if nReduce <= 0 {
		nReduce = runtime.NumCPU()
	}
	coordinator := mr.MakeCoordinator(files, nReduce)
	mapf := distributed_computing.Map
	reducef := distributed_computing.Reduce
	mr.Worker(mapf, reducef)
	for !coordinator.Done() {
		time.Sleep(askJobTime)
	}

	// aa := time.Now()
	// var aa time.Time
	// aa = time.Time("2023-05-31 15:41:22")

	// ===============================================================

	// aa, err := time.ParseInLocation("2006-01-02 15:04:05", "2023-05-31 15:41:22", time.Local)
	//
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//
	// fmt.Println(aa)

	// price, err := decimal.NewFromString("136.02")
	// if err != nil {
	// 	panic(err)
	// }
	// quantity := decimal.NewFromInt(3)
	// fee, _ := decimal.NewFromString(".035")
	// taxRate, _ := decimal.NewFromString(".08875")
	// subtotal := price.Mul(quantity)
	// preTax := subtotal.Mul(fee.Add(decimal.NewFromFloat(1)))
	// total := preTax.Mul(taxRate.Add(decimal.NewFromFloat(1)))
	// fmt.Println("Subtotal:", subtotal)                      // Subtotal: 408.06
	// fmt.Println("Pre-tax:", preTax)                         // Pre-tax: 422.3421
	// fmt.Println("Taxes:", total.Sub(preTax))                // Taxes: 37.482861375
	// fmt.Println("Total:", total)                            // Total: 459.824961375
	// fmt.Println("Tax rate:", total.Sub(preTax).Div(preTax)) // Tax rate: 0.08875
}
