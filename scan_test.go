package hbase

import (
	"runtime"
	"strconv"
	"sync"
	"time"

	. "github.com/pingcap/check"
)

type ScanTestSuit struct {
	cli       HBaseClient
	tableName string
}

var _ = Suite(&ScanTestSuit{})

func (s *ScanTestSuit) SetUpSuite(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, IsNil)

	s.tableName = "test_scan"
}

func (s *ScanTestSuit) TearDownSuite(c *C) {
}

func (s *ScanTestSuit) SetUpTest(c *C) {
	tblDesc := NewTableDesciptor(s.tableName)
	cf := newColumnFamilyDescriptor("cf", 3)
	tblDesc.AddColumnDesc(cf)
	err := s.cli.CreateTable(tblDesc, nil)
	c.Assert(err, IsNil)
}

func (s *ScanTestSuit) TearDownTest(c *C) {
	err := s.cli.DisableTable(s.tableName)
	c.Assert(err, IsNil)

	err = s.cli.DropTable(s.tableName)
	c.Assert(err, IsNil)
}

func (s *ScanTestSuit) TestNextKey(c *C) {
	inputs := []struct {
		data   []byte
		result []byte
	}{
		{nil, []byte{0}},
		{[]byte{}, []byte{0}},
		{[]byte{0}, []byte{1}},
		{[]byte{1, 2, 3}, []byte{1, 2, 4}},
		{[]byte{1, 255}, []byte{2, 0}},
		{[]byte{255}, []byte{0, 0}},
		{[]byte{255, 255}, []byte{0, 0, 0}},
	}

	for _, input := range inputs {
		result := nextKey(input.data)
		c.Assert(result, BytesEquals, input.result, Commentf("%v - %v - %v", input.data, input.result, result))
	}
}

func (s *ScanTestSuit) TestScan(c *C) {
	for i := 1; i <= 10000; i++ {
		p := NewPut([]byte(strconv.Itoa(i)))
		p.AddValue([]byte("cf"), []byte("qscan"), []byte(strconv.Itoa(i)))
		ok, err := s.cli.Put(s.tableName, p)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scan := NewScan([]byte(s.tableName), 100, s.cli)
			defer scan.Close()
			// [1, 5)
			scan.StartRow = []byte("1")
			scan.StopRow = []byte("5")

			cnt := 0
			for {
				r := scan.Next()
				if r == nil || scan.Closed() {
					break
				}
				cnt++
			}
			c.Assert(cnt, Equals, 4445)
		}()
	}

	wg.Wait()

	for i := 1; i <= 10000; i++ {
		d := NewDelete([]byte(strconv.Itoa(i)))
		b, err := s.cli.Delete(s.tableName, d)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)
	}
}

func (s *ScanTestSuit) TestScanWithVersion(c *C) {
	for i := 1; i <= 10000; i++ {
		p := NewPut([]byte(strconv.Itoa(i)))
		p.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))
		ok, err := s.cli.Put(s.tableName, p)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
	}
	// Test version put.
	p := NewPut([]byte("99999"))
	p.AddValue([]byte("cf"), []byte("q"), []byte("1002"))
	p.AddTimestamp(1002)
	ok, err := s.cli.Put(s.tableName, p)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	p = NewPut([]byte("99999"))
	p.AddValue([]byte("cf"), []byte("q"), []byte("1003"))
	p.AddTimestamp(1003)
	ok, err = s.cli.Put(s.tableName, p)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	p = NewPut([]byte("99999"))
	p.AddValue([]byte("cf"), []byte("q"), []byte("1001"))
	p.AddTimestamp(1001)
	ok, err = s.cli.Put(s.tableName, p)
	c.Assert(ok, IsTrue)
	c.Assert(err, IsNil)

	// Test version get.
	g := NewGet([]byte("99999"))
	g.AddColumn([]byte("cf"), []byte("q"))
	g.AddTimeRange(1001, 1004)
	r, err := s.cli.Get(s.tableName, g)
	c.Assert(r.Columns["cf:q"].Values, HasLen, 1)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "1003")
	c.Assert(err, IsNil)

	g = NewGet([]byte("99999"))
	g.AddColumn([]byte("cf"), []byte("q"))
	g.AddTimeRange(1001, 1003)
	r, err = s.cli.Get(s.tableName, g)
	c.Assert(r.Columns["cf:q"].Values, HasLen, 1)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "1002")
	c.Assert(err, IsNil)

	g = NewGet([]byte("99999"))
	g.AddColumn([]byte("cf"), []byte("q"))
	g.AddTimeRange(999, 1001)
	r, err = s.cli.Get(s.tableName, g)
	c.Assert(r, IsNil)
	c.Assert(err, IsNil)

	g = NewGet([]byte("99999"))
	g.AddColumn([]byte("cf"), []byte("q"))
	r, err = s.cli.Get(s.tableName, g)
	c.Assert(err, IsNil)
	c.Assert(r.Columns["cf:q"].Values, HasLen, 1)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "1003")
	c.Assert(r.SortedColumns[0].Ts, Equals, uint64(1003))

	g = NewGet([]byte("99999"))
	g.AddColumn([]byte("cf"), []byte("q"))
	g.Versions = 2
	r, err = s.cli.Get(s.tableName, g)
	c.Assert(r.Columns["cf:q"].Values, HasLen, 2)
	value, ok := r.Columns["cf:q"].Values[1003]
	c.Assert(ok, IsTrue)
	c.Assert(string(value), Equals, "1003")
	value, ok = r.Columns["cf:q"].Values[1002]
	c.Assert(ok, IsTrue)
	c.Assert(string(value), Equals, "1002")

	g = NewGet([]byte("99999"))
	g.AddColumn([]byte("cf"), []byte("q"))
	g.Versions = 5
	r, err = s.cli.Get(s.tableName, g)
	c.Assert(r.Columns["cf:q"].Values, HasLen, 3)
	value, ok = r.Columns["cf:q"].Values[1003]
	c.Assert(ok, IsTrue)
	c.Assert(string(value), Equals, "1003")
	value, ok = r.Columns["cf:q"].Values[1002]
	c.Assert(ok, IsTrue)
	c.Assert(string(value), Equals, "1002")
	value, ok = r.Columns["cf:q"].Values[1001]
	c.Assert(ok, IsTrue)
	c.Assert(string(value), Equals, "1001")

	g = NewGet([]byte("99999"))
	g.AddColumn([]byte("cf"), []byte("q"))
	g.Versions = 3
	g.AddTimeRange(999, 1002)
	r, err = s.cli.Get(s.tableName, g)
	c.Assert(r.Columns["cf:q"].Values, HasLen, 1)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "1001")
	c.Assert(r.SortedColumns[0].Ts, Equals, uint64(1001))

	// Test version scan.
	// Range -> [9999, 999999)
	scan := NewScan([]byte(s.tableName), 100, s.cli)
	scan.StartRow = []byte("9999")
	scan.StopRow = []byte("999999")

	r = scan.Next()
	c.Assert(r.SortedColumns, HasLen, 1)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "9999")

	r = scan.Next()
	c.Assert(r.SortedColumns, HasLen, 1)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "1003")

	r = scan.Next()
	c.Assert(r, IsNil)
	scan.Close()

	// Range -> [9999, 999999)
	// Version -> [1000, 1002)
	scan = NewScan([]byte(s.tableName), 100, s.cli)
	scan.StartRow = []byte("9999")
	scan.StopRow = []byte("999999")
	scan.AddTimeRange(1000, 1002)

	r = scan.Next()
	c.Assert(r.SortedColumns, HasLen, 1)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "1001")

	r = scan.Next()
	c.Assert(r, IsNil)
	scan.Close()

	for i := 1; i <= 10000; i++ {
		d := NewDelete([]byte(strconv.Itoa(i)))
		b, err := s.cli.Delete(s.tableName, d)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)
	}
}

func (s *ScanTestSuit) TestScanCrossMultiRegions(c *C) {
	origData := map[string]map[string]string{}
	for i := 1; i <= 2000; i++ {
		p := NewPut([]byte(strconv.Itoa(i)))
		p.AddValue([]byte("cf"), []byte("qmulti"), []byte(strconv.Itoa(i)))
		ok, err := s.cli.Put(s.tableName, p)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)

		rowKey := string(p.Row)
		origData[rowKey] = map[string]string{}
		origData[rowKey]["cf:qmulti"] = string(p.Values[0][0])
	}
	err := s.cli.Split(s.tableName, "1024")
	c.Assert(err, IsNil)
	// Sleep wait Split finish.
	time.Sleep(1 * time.Second)

	scan := NewScan([]byte(s.tableName), 100, s.cli)
	cnt := 0
	for i := 1; i <= 2000; i++ {
		r := scan.Next()
		c.Assert(r, NotNil)
		rowKey := string(r.Row)
		origRow, ok := origData[rowKey]
		c.Assert(ok, IsTrue)
		c.Assert(origRow, NotNil)
		for column := range r.Columns {
			origValue, ok := origRow[column]
			c.Assert(ok, IsTrue)
			value := string(r.Columns[column].Value)
			c.Assert(origValue, Equals, value)
		}
		delete(origData, rowKey)
		cnt++
	}
	c.Assert(cnt, Equals, 2000)

	for i := 1; i <= 2000; i++ {
		d := NewDelete([]byte(strconv.Itoa(i)))
		b, err := s.cli.Delete(s.tableName, d)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)
	}
}

func (s *ScanTestSuit) TestScanWhileSplit(c *C) {
	origData := map[string]map[string]string{}
	for i := 1; i <= 3000; i++ {
		p := NewPut([]byte(strconv.Itoa(i)))
		p.AddValue([]byte("cf"), []byte("qsplit"), []byte(strconv.Itoa(i)))
		ok, err := s.cli.Put(s.tableName, p)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)

		rowKey := string(p.Row)
		origData[rowKey] = map[string]string{}
		origData[rowKey]["cf:qsplit"] = string(p.Values[0][0])
	}
	scan := NewScan([]byte(s.tableName), 100, s.cli)
	cnt := 0
	// Scan to a random row, like 1987 or whatever.
	for i := 1; i <= 1987; i++ {
		r := scan.Next()
		c.Assert(r, NotNil)
		rowKey := string(r.Row)
		origRow, ok := origData[rowKey]
		c.Assert(ok, IsTrue)
		c.Assert(origRow, NotNil)
		for column := range r.Columns {
			origValue, ok := origRow[column]
			c.Assert(ok, IsTrue)
			value := string(r.Columns[column].Value)
			c.Assert(origValue, Equals, value)
		}
		delete(origData, rowKey)
		cnt++
	}
	c.Assert(cnt, Equals, 1987)
	// At this time, regions is splitting.
	err := s.cli.Split(s.tableName, "2048")
	c.Assert(err, IsNil)
	// Sleep wait Split finish.
	time.Sleep(1 * time.Second)

	// Scan is go on, and get data normally.
	for i := 1988; i <= 3000; i++ {
		r := scan.Next()
		c.Assert(r, NotNil)
		rowKey := string(r.Row)
		origRow, ok := origData[rowKey]
		c.Assert(ok, IsTrue)
		c.Assert(origRow, NotNil)
		for column := range r.Columns {
			origValue, ok := origRow[column]
			c.Assert(ok, IsTrue)
			value := string(r.Columns[column].Value)
			c.Assert(origValue, Equals, value)
		}
		for column := range r.Columns {
			origValue, ok := origRow[column]
			value := string(r.Columns[column].Value)
			c.Assert(ok, IsTrue)
			c.Assert(origValue, Equals, value)
		}
		delete(origData, rowKey)
		cnt++
	}
	c.Assert(cnt, Equals, 3000)

	for i := 1; i <= 3000; i++ {
		d := NewDelete([]byte(strconv.Itoa(i)))
		b, err := s.cli.Delete(s.tableName, d)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)
	}
}

func (s *ScanTestSuit) TestScanClose(c *C) {
	for i := 1; i <= 100; i++ {
		p := NewPut([]byte(strconv.Itoa(i)))
		p.AddValue([]byte("cf"), []byte("qclose"), []byte(strconv.Itoa(i)))
		ok, err := s.cli.Put(s.tableName, p)
		c.Assert(ok, IsTrue)
		c.Assert(err, IsNil)
	}

	// Do not get any data.
	scan := NewScan([]byte(s.tableName), 100, s.cli)
	err := scan.Close()
	c.Assert(err, IsNil)
	r := scan.Next()
	c.Assert(r, IsNil)

	// Get some data.
	scan = NewScan([]byte(s.tableName), 100, s.cli)
	r = scan.Next()
	c.Assert(r, NotNil)

	// If scanner Close, then do not fetch any data even cache still has some.
	err = scan.Close()
	c.Assert(err, IsNil)
	r = scan.Next()
	c.Assert(r, IsNil)

	for i := 1; i <= 100; i++ {
		d := NewDelete([]byte(strconv.Itoa(i)))
		b, err := s.cli.Delete(s.tableName, d)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)
	}
}
