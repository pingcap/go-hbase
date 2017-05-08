package hbase

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
)

type AdminTestSuit struct {
	cli              HBaseClient
	tableName        string
	invalidTableName string
}

var (
	testZks = flag.String("zk", "localhost", "hbase zookeeper info")
)

func getTestZkHosts() []string {
	zks := strings.Split(*testZks, ",")
	if len(zks) == 0 {
		log.Fatal("invalid zk path")
	}
	return zks
}

var _ = Suite(&AdminTestSuit{})

func (s *AdminTestSuit) SetUpSuite(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, IsNil)

	s.tableName = "test_admin"
	s.invalidTableName = "test_admin_xxx"
}

func (s *AdminTestSuit) TearDownSuite(c *C) {
}

func (s *AdminTestSuit) SetUpTest(c *C) {
	tblDesc := NewTableDesciptor(s.tableName)
	cf := NewColumnFamilyDescriptor("cf")
	tblDesc.AddColumnDesc(cf)
	err := s.cli.CreateTable(tblDesc, [][]byte{[]byte("f"), []byte("e"), []byte("c")})
	c.Assert(err, IsNil)
}

func (s *AdminTestSuit) TearDownTest(c *C) {
	err := s.cli.DisableTable(s.tableName)
	c.Assert(err, IsNil)
	err = s.cli.DropTable(s.tableName)
	c.Assert(err, IsNil)
}

func (s *AdminTestSuit) TestTable(c *C) {
	b, err := s.cli.TableExists(s.tableName)
	c.Assert(err, IsNil)
	c.Assert(b, IsTrue)

	err = s.cli.DisableTable(s.tableName)
	c.Assert(err, IsNil)

	// Wait for table disabled.
	time.Sleep(3 * time.Second)

	p := NewPut([]byte("key"))
	p.AddValue([]byte("cf"), []byte("f"), []byte("value"))
	ok, err := s.cli.Put(s.tableName, p)
	c.Assert(err, NotNil)
	c.Assert(ok, IsFalse)

	// Wait for table enabled.
	time.Sleep(3 * time.Second)

	err = s.cli.EnableTable(s.tableName)
	c.Assert(err, IsNil)

	ok, err = s.cli.Put(s.tableName, p)
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)

	g := NewGet([]byte("key"))
	g.AddColumn([]byte("cf"), []byte("f"))
	r, err := s.cli.Get(s.tableName, g)
	c.Assert(err, IsNil)
	c.Assert(r.Columns["cf:f"].Values, HasLen, 1)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "value")

	// Test check unexisted table.
	b, err = s.cli.TableExists(s.invalidTableName)
	c.Assert(err, IsNil)
	c.Assert(b, IsFalse)

	// Test disable unexisted table.
	err = s.cli.DisableTable(s.invalidTableName)
	c.Assert(err, NotNil)

	// Test enable unexisted table.
	err = s.cli.EnableTable(s.invalidTableName)
	c.Assert(err, NotNil)

	// Test drop unexisted table.
	err = s.cli.DropTable(s.invalidTableName)
	c.Assert(err, NotNil)
}

func (s *AdminTestSuit) TestCreateTableAsync(c *C) {
	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			tblName := fmt.Sprintf("f_%d", i)
			tblDesc := NewTableDesciptor(tblName)
			cf := NewColumnFamilyDescriptor("cf")
			tblDesc.AddColumnDesc(cf)
			tblDesc.AddColumnDesc(cf)
			b, err := s.cli.TableExists(tblName)
			c.Assert(err, IsNil)
			if b {
				// Maybe some table is in disabled state, so we must ignore this error.
				s.cli.DisableTable(tblName)
				err = s.cli.DropTable(tblName)
				c.Assert(err, IsNil)
			}
			err = s.cli.CreateTable(tblDesc, nil)
			c.Assert(err, IsNil)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		tblName := fmt.Sprintf("f_%d", i)
		b, err := s.cli.TableExists(tblName)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)

		err = s.cli.DisableTable(tblName)
		c.Assert(err, IsNil)

		err = s.cli.DropTable(tblName)
		c.Assert(err, IsNil)
	}
}

func (s *AdminTestSuit) TestGetPauseTime(c *C) {
	invalidRetry := -1
	c.Assert(getPauseTime(invalidRetry), Equals, retryPauseTime[0]*defaultRetryWaitMs)
	invalidRetry = len(retryPauseTime)
	c.Assert(getPauseTime(invalidRetry), Equals, retryPauseTime[len(retryPauseTime)-1]*defaultRetryWaitMs)
}

func (s *AdminTestSuit) TestGetRegions(c *C) {
	regions, err := s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
}

func (s *AdminTestSuit) TestTableAutoSplit(c *C) {
	regions, err := s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
	origData := map[string]map[string]string{}
	prefixLower := 'b'
	prefixUpper := 'f'
	for prefix := prefixLower; prefix < prefixUpper; prefix++ {
		// b_0, b_1, ...
		// If insert few row, it may not be splited even invoke Split explicitly.
		for i := 0; i < 10000; i++ {
			p := NewPut([]byte(fmt.Sprintf("%c_%d", prefix, i)))
			p.AddStringValue("cf", "c", fmt.Sprintf("%c%c_%d", prefix, prefix, i))
			rowKey := string(p.Row)
			origData[rowKey] = map[string]string{}
			origData[rowKey]["cf:c"] = string(p.Values[0][0])
			b, err := s.cli.Put(s.tableName, p)
			c.Assert(err, IsNil)
			c.Assert(b, IsTrue)
		}
	}
	err = s.cli.Split(s.tableName, "")
	c.Assert(err, IsNil)
	// Sleep wait Split finish.
	time.Sleep(5 * time.Second)

	regions, err = s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	// After insert 10K data,
	// with Themis coprocessor, it will be split to 7 regions,
	// without themis, that number will be 6.
	// Split depends on coprocessor behavior and hbase conf,
	// so we just know it will split at least one region.
	c.Assert(len(regions), Greater, 4)

	// Check all data are still.
	scan := NewScan([]byte(s.tableName), 1000, s.cli)
	cnt := 0
	for prefix := prefixLower; prefix < prefixUpper; prefix++ {
		for i := 0; i < 10000; i++ {
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
	}
	c.Assert(cnt, Equals, int(prefixUpper-prefixLower)*10000)
}

func (s *AdminTestSuit) TestTableSplit(c *C) {
	regions, err := s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 4)
	for i := 0; i < 100; i++ {
		p := NewPut([]byte(fmt.Sprintf("b_%d", i)))
		p.AddStringValue("cf", "c", fmt.Sprintf("bb_%d", i))
		b, err := s.cli.Put(s.tableName, p)
		c.Assert(err, IsNil)
		c.Assert(b, IsTrue)
	}
	err = s.cli.Split(s.tableName, "b_2")
	c.Assert(err, IsNil)

	// Sleep wait Split finish.
	time.Sleep(500 * time.Millisecond)

	regions, err = s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 5)

	// Test directly split region.
	err = s.cli.Split(regions[1].Name, "b_50")
	c.Assert(err, IsNil)

	// Sleep wait Split finish.
	time.Sleep(500 * time.Millisecond)

	regions, err = s.cli.GetRegions([]byte(s.tableName), false)
	c.Assert(err, IsNil)
	c.Assert(regions, HasLen, 6)

}
