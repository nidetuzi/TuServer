package randutil_test

import (
	"fmt"

	"tuserv/pkg/randutil"
)

func ExampleNewSeed() {
	r := randutil.NewSeed(1)
	n, err := r.Int(1, 6)
	if err != nil {
		panic(err)
	}
	pool := []randutil.Entry[string]{
		{Item: "金币", Weight: 70},
		{Item: "钻石", Weight: 20},
		{Item: "武器", Weight: 10},
		{Item: "未开放", Weight: 0},
	}
	one, err := randutil.Weighted(r, pool)
	if err != nil {
		panic(err)
	}
	two, err := randutil.WeightedUnique(r, pool, 2)
	if err != nil {
		panic(err)
	}
	fmt.Println(n >= 1 && n <= 6)
	fmt.Println(one)
	fmt.Println(two)
	// Output:
	// true
	// 金币
	// [金币 钻石]
}
