package store_test

import (
	"fmt"

	"github.com/guoyyit/time-series-codec-go/codec"
	"github.com/guoyyit/time-series-codec-go/store"
)

func Example() {
	s, err := store.Open(store.Config{
		Dir:            "/tmp/tsdb-example",
		Codec:          codec.Config{Codec: codec.CodecLossless},
		MaxBlockPoints: 1024,
	})
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// writes may be out of order and duplicated
	s.Write("cpu.usage",
		codec.Point{TS: 3000, V: 0.9},
		codec.Point{TS: 1000, V: 0.5},
		codec.Point{TS: 2000, V: 0.7},
		codec.Point{TS: 2000, V: 0.8}, // duplicate: last write wins
	)

	pts, _ := s.Query("cpu.usage", 0, 4000)
	for _, p := range pts {
		fmt.Println(p.TS, p.V)
	}
	// Output:
	// 1000 0.5
	// 2000 0.8
	// 3000 0.9
}
