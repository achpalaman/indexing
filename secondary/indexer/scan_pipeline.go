// Copyright (c) 2014 Couchbase, Inc.

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package indexer

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/indexing/secondary/collatejson"
	c "github.com/couchbase/indexing/secondary/common"
	l "github.com/couchbase/indexing/secondary/logging"
	p "github.com/couchbase/indexing/secondary/pipeline"
	"github.com/couchbase/query/expression"
	"github.com/couchbase/query/value"
)

var (
	ErrLimitReached = errors.New("Row limit reached")
	encodedNull     = []byte{2, 0}
	encodedZero     = []byte{5, 48, 0}
)

type ScanPipeline struct {
	src    p.Source
	object p.Pipeline
	req    *ScanRequest
	config c.Config

	aggrRes *aggrResult

	rowsReturned  uint64
	bytesRead     uint64
	rowsScanned   uint64
	cacheHitRatio int
}

func (p *ScanPipeline) Cancel(err error) {
	p.src.Shutdown(err)
}

func (p *ScanPipeline) Execute() error {
	return p.object.Execute()
}

func (p ScanPipeline) RowsReturned() uint64 {
	return p.rowsReturned
}

func (p ScanPipeline) BytesRead() uint64 {
	return p.bytesRead
}

func (p ScanPipeline) RowsScanned() uint64 {
	return p.rowsScanned
}

func (p ScanPipeline) CacheHitRatio() int {
	return p.cacheHitRatio
}

func NewScanPipeline(req *ScanRequest, w ScanResponseWriter, is IndexSnapshot, cfg c.Config) *ScanPipeline {
	scanPipeline := new(ScanPipeline)
	scanPipeline.req = req
	scanPipeline.config = cfg

	src := &IndexScanSource{is: is, p: scanPipeline}
	src.InitWriter()
	dec := &IndexScanDecoder{p: scanPipeline}
	dec.InitReadWriter()
	wr := &IndexScanWriter{w: w, p: scanPipeline}
	wr.InitReader()

	dec.SetSource(src)
	wr.SetSource(dec)

	scanPipeline.src = src
	scanPipeline.object.AddSource("source", src)
	scanPipeline.object.AddFilter("decoder", dec)
	scanPipeline.object.AddSink("writer", wr)

	if req.GroupAggr != nil {
		scanPipeline.aggrRes = &aggrResult{}
	}

	return scanPipeline

}

type IndexScanSource struct {
	p.ItemWriter
	is IndexSnapshot
	p  *ScanPipeline
}

type IndexScanDecoder struct {
	p.ItemReadWriter
	p *ScanPipeline
}

type IndexScanWriter struct {
	p.ItemReader
	w ScanResponseWriter
	p *ScanPipeline
}

func (s *IndexScanSource) Routine() error {
	var err error
	defer s.CloseWrite()

	r := s.p.req
	var currentScan Scan
	currOffset := int64(0)
	count := 1
	checkDistinct := r.Distinct && !r.isPrimary

	buf := secKeyBufPool.Get() //Composite element filtering
	r.keyBufList = append(r.keyBufList, buf)
	buf2 := secKeyBufPool.Get() //Tracking for distinct
	r.keyBufList = append(r.keyBufList, buf2)
	previousRow := (*buf2)[:0]
	buf3 := secKeyBufPool.Get() //Decoding in ExplodeArray2
	r.keyBufList = append(r.keyBufList, buf3)
	docidbuf := make([]byte, 1024)
	revbuf := secKeyBufPool.Get() //Reverse collation buffer
	r.keyBufList = append(r.keyBufList, revbuf)

	var cktmp, dktmp [][]byte
	cktmp = make([][]byte, len(s.p.req.IndexInst.Defn.SecExprs))

	var cachedEntry entryCache

	if r.GroupAggr != nil {
		r.GroupAggr.groups = make([]*groupKey, len(r.GroupAggr.Group))
		for i, _ := range r.GroupAggr.Group {
			r.GroupAggr.groups[i] = new(groupKey)
		}

		r.GroupAggr.aggrs = make([]*aggrVal, len(r.GroupAggr.Aggrs))
		for i, _ := range r.GroupAggr.Aggrs {
			r.GroupAggr.aggrs[i] = new(aggrVal)
		}

		if r.GroupAggr.NeedDecode {
			dktmp = make([][]byte, len(s.p.req.IndexInst.Defn.SecExprs))
		}

	}

	hasDesc := s.p.req.IndexInst.Defn.HasDescending()

	iterCount := 0
	fn := func(entry []byte) error {
		if iterCount%SCAN_ROLLBACK_ERROR_BATCHSIZE == 0 && r.hasRollback != nil && r.hasRollback.Load() == true {
			return ErrIndexRollback
		}
		iterCount++
		s.p.rowsScanned++

		skipRow := false
		var ck, dk [][]byte

		//get the key in original format
		if hasDesc {
			revbuf := (*revbuf)[:0]
			//copy is required, otherwise storage may get updated if storage
			//returns pointer to original item(e.g. memdb)
			revbuf = append(revbuf, entry...)
			jsonEncoder.ReverseCollate(revbuf, s.p.req.IndexInst.Defn.Desc)
			entry = revbuf
		}

		if currentScan.ScanType == FilterRangeReq {
			if len(entry) > cap(*buf) {
				*buf = make([]byte, 0, len(entry)+1024)
				*buf3 = make([]byte, len(entry)+1024)
			}
			getDecoded := (r.GroupAggr != nil && r.GroupAggr.NeedDecode)
			skipRow, ck, dk, err = filterScanRow2(entry, currentScan,
				(*buf)[:0], *buf3, getDecoded, cktmp, dktmp, r, &cachedEntry)
			if err != nil {
				return err
			}
		}

		if skipRow {
			return nil
		}

		if !r.isPrimary {
			e := secondaryIndexEntry(entry)
			count = e.Count()
		}

		if r.GroupAggr != nil {

			if ck == nil && len(entry) > cap(*buf) {
				*buf = make([]byte, 0, len(entry)+1024)
			}

			var docid []byte
			if r.isPrimary {
				docid = entry
			} else if r.GroupAggr.DependsOnPrimaryKey {
				docid, err = secondaryIndexEntry(entry).ReadDocId((docidbuf)[:0]) //docid for N1QLExpr evaluation for Group/Aggr
				if err != nil {
					return err
				}
			}

			err = computeGroupAggr(ck, dk, count, docid, entry, (*buf)[:0], *buf3, s.p.aggrRes, r.GroupAggr, cktmp, dktmp, &cachedEntry, r)
			if err != nil {
				return err
			}
			count = 1 //reset count; count is used for aggregates computation
		}

		if r.Indexprojection != nil && r.Indexprojection.projectSecKeys {
			if r.GroupAggr != nil {
				entry, err = projectGroupAggr((*buf)[:0], r.Indexprojection, s.p.aggrRes, r.isPrimary)
				if entry == nil {
					return err
				}
			} else if !r.isPrimary {
				if ck == nil && len(entry) > cap(*buf) {
					*buf = make([]byte, 0, len(entry)+1024)
				}

				entry, err = projectKeys(ck, entry, (*buf)[:0], r.Indexprojection, cktmp)
			}
			if err != nil {
				return err
			}
		}

		if checkDistinct {
			if len(previousRow) != 0 && distinctCompare(entry, previousRow) {
				return nil // Ignore the entry as it is same as previous entry
			}
		}

		for i := 0; i < count; i++ {
			if r.Distinct && i > 0 {
				break
			}
			if currOffset >= r.Offset {
				s.p.rowsReturned++
				wrErr := s.WriteItem(entry)
				if wrErr != nil {
					return wrErr
				}
				if s.p.rowsReturned == uint64(r.Limit) {
					return ErrLimitReached
				}
			} else {
				currOffset++
			}
		}

		if checkDistinct {
			previousRow = append(previousRow[:0], entry...)
		}

		return nil
	}

	sliceSnapshots, err1 := GetSliceSnapshots(s.is, s.p.req.PartitionIds)
	if err1 != nil {
		return err1
	}

	if r.GroupAggr != nil {
		if r.GroupAggr.IsLeadingGroup {
			s.p.aggrRes.SetMaxRows(1)
		} else {
			s.p.aggrRes.SetMaxRows(s.p.config["scan.partial_group_buffer_size"].Int())
		}
	}

loop:
	for _, scan := range r.Scans {
		currentScan = scan
		err = scatter(r, scan, sliceSnapshots, fn, s.p.config)
		switch err {
		case nil:
		case p.ErrSupervisorKill, ErrLimitReached:
			break loop
		default:
			s.CloseWithError(err)
			break loop
		}
	}

	s.p.cacheHitRatio = cachedEntry.CacheHitRatio()

	if r.GroupAggr != nil && err == nil {

		for _, r := range s.p.aggrRes.rows {
			r.SetFlush(true)
		}

		for {
			entry, err := projectGroupAggr((*buf)[:0], r.Indexprojection, s.p.aggrRes, r.isPrimary)
			if err != nil {
				s.CloseWithError(err)
				break
			}

			if entry == nil {

				if s.p.rowsReturned == 0 {

					//handle special group rules
					entry, err = projectEmptyResult((*buf)[:0], r.Indexprojection, r.GroupAggr)
					if err != nil {
						s.CloseWithError(err)
						break
					}

					if entry == nil {
						return nil
					}

					s.p.rowsReturned++
					wrErr := s.WriteItem(entry)
					if wrErr != nil {
						s.CloseWithError(wrErr)
						break
					}
				}
				return nil
			}

			if err != nil {
				s.CloseWithError(err)
				break
			}

			if currOffset >= r.Offset {
				s.p.rowsReturned++
				wrErr := s.WriteItem(entry)
				if wrErr != nil {
					s.CloseWithError(wrErr)
					break
				}
				if s.p.rowsReturned == uint64(r.Limit) {
					return nil
				}
			} else {
				currOffset++
			}

		}

	}

	return nil
}

func (d *IndexScanDecoder) Routine() error {
	defer d.CloseWrite()
	defer d.CloseRead()

	var sk, docid []byte
	tmpBuf := p.GetBlock()
	defer p.PutBlock(tmpBuf)

loop:
	for {
		row, err := d.ReadItem()
		switch err {
		case nil:
		case p.ErrNoMoreItem, p.ErrSupervisorKill:
			break loop
		default:
			d.CloseWithError(err)
			break loop
		}

		if len(row)*3 > cap(*tmpBuf) {
			(*tmpBuf) = make([]byte, len(row)*3, len(row)*3)
		}

		t := (*tmpBuf)[:0]
		if d.p.req.GroupAggr != nil {
			sk, _ = jsonEncoder.Decode(row, t)
		} else if d.p.req.isPrimary {
			sk, docid = piSplitEntry(row, t)
		} else {
			sk, docid, _ = siSplitEntry(row, t)
		}

		d.p.bytesRead += uint64(len(sk) + len(docid))
		if !d.p.req.isPrimary && !d.p.req.projectPrimaryKey {
			docid = nil
		}
		err = d.WriteItem(sk, docid)
		if err != nil {
			break // TODO: Old code. Should it be ClosedWithError?
		}
	}

	return nil
}

func (d *IndexScanWriter) Routine() error {
	var err error
	var sk, pk []byte

	defer func() {
		// Send error to the client if not client requested cancel.
		if err != nil && err.Error() != c.ErrClientCancel.Error() {
			d.w.Error(err)
		}
		d.CloseRead()
	}()

loop:
	for {
		sk, err = d.ReadItem()
		switch err {
		case nil:
		case p.ErrNoMoreItem:
			err = nil
			break loop
		default:
			break loop
		}

		pk, err = d.ReadItem()
		if err != nil {
			return err
		}

		if err = d.w.Row(pk, sk); err != nil {
			return err
		}

		/*
		   TODO(sarath): Use block chunk send protocol
		   Instead of collecting rows and encoding into protobuf,
		   we can send full 16kb block.

		       b, err := d.PeekBlock()
		       if err == p.ErrNoMoreItem {
		           d.CloseRead()
		           return nil
		       }

		       d.W.RawBytes(b)
		       d.FlushBlock()
		*/
	}

	return err
}

func piSplitEntry(entry []byte, tmp []byte) ([]byte, []byte) {
	e := primaryIndexEntry(entry)
	sk, err := e.ReadSecKey(tmp)
	c.CrashOnError(err)
	docid, err := e.ReadDocId(sk)
	return sk, docid[len(sk):]
}

func siSplitEntry(entry []byte, tmp []byte) ([]byte, []byte, int) {
	e := secondaryIndexEntry(entry)
	sk, err := e.ReadSecKey(tmp)
	c.CrashOnError(err)
	docid, err := e.ReadDocId(sk)
	c.CrashOnError(err)
	count := e.Count()
	return sk, docid[len(sk):], count
}

// Return true if the row needs to be skipped based on the filter
func filterScanRow(key []byte, scan Scan, buf []byte) (bool, [][]byte, error) {
	var compositekeys [][]byte
	var err error

	compositekeys, err = jsonEncoder.ExplodeArray(key, buf)
	if err != nil {
		return false, nil, err
	}

	var filtermatch bool
	for _, filtercollection := range scan.Filters {
		if len(filtercollection.CompositeFilters) > len(compositekeys) {
			// There cannot be more ranges than number of composite keys
			err = errors.New("There are more ranges than number of composite elements in the index")
			return false, nil, err
		}
		filtermatch = applyFilter(compositekeys, filtercollection.CompositeFilters)
		if filtermatch {
			return false, compositekeys, nil
		}
	}

	return true, compositekeys, nil
}

// Return true if the row needs to be skipped based on the filter
func filterScanRow2(key []byte, scan Scan, buf, decbuf []byte, getDecoded bool,
	cktmp, dktmp [][]byte, r *ScanRequest, cachedEntry *entryCache) (bool, [][]byte, [][]byte, error) {

	var compositekeys, decodedkeys [][]byte
	var err error

	if !r.isPrimary && cachedEntry.Exists() {
		if cachedEntry.EqualsEntry(key) {
			compositekeys, decodedkeys = cachedEntry.Get()
			cachedEntry.SetValid(true)
		} else {
			cachedEntry.SetValid(false)
		}
	}

	if compositekeys == nil {
		compositekeys, decodedkeys, err = jsonEncoder.ExplodeArray2(key, buf, decbuf, cktmp, dktmp)
		if err != nil {
			return false, nil, nil, err
		}
	}

	if !cachedEntry.Exists() {
		cachedEntry.Init(r, getDecoded, len(compositekeys))
	}
	if !cachedEntry.Valid() {
		cachedEntry.Update(key, compositekeys, decodedkeys)
	}

	var filtermatch bool
	for _, filtercollection := range scan.Filters {
		if len(filtercollection.CompositeFilters) > len(compositekeys) {
			// There cannot be more ranges than number of composite keys
			err = errors.New("There are more ranges than number of composite elements in the index")
			return false, nil, nil, err
		}
		filtermatch = applyFilter(compositekeys, filtercollection.CompositeFilters)
		if filtermatch {
			return false, compositekeys, decodedkeys, nil
		}
	}

	return true, compositekeys, decodedkeys, nil
}

// Return true if filter matches the composite keys
func applyFilter(compositekeys [][]byte, compositefilters []CompositeElementFilter) bool {

	for i, filter := range compositefilters {
		ck := compositekeys[i]
		checkLow := (filter.Low != MinIndexKey)
		checkHigh := (filter.High != MaxIndexKey)

		switch filter.Inclusion {
		case Neither:
			// if ck > low and ck < high
			if checkLow {
				if !(bytes.Compare(ck, filter.Low.Bytes()) > 0) {
					return false
				}
			}
			if checkHigh {
				if !(bytes.Compare(ck, filter.High.Bytes()) < 0) {
					return false
				}
			}
		case Low:
			// if ck >= low and ck < high
			if checkLow {
				if !(bytes.Compare(ck, filter.Low.Bytes()) >= 0) {
					return false
				}
			}
			if checkHigh {
				if !(bytes.Compare(ck, filter.High.Bytes()) < 0) {
					return false
				}
			}
		case High:
			// if ck > low and ck <= high
			if checkLow {
				if !(bytes.Compare(ck, filter.Low.Bytes()) > 0) {
					return false
				}
			}
			if checkHigh {
				if !(bytes.Compare(ck, filter.High.Bytes()) <= 0) {
					return false
				}
			}
		case Both:
			// if ck >= low and ck <= high
			if checkLow {
				if !(bytes.Compare(ck, filter.Low.Bytes()) >= 0) {
					return false
				}
			}
			if checkHigh {
				if !(bytes.Compare(ck, filter.High.Bytes()) <= 0) {
					return false
				}
			}
		}
	}

	return true
}

// Compare secondary entries and return true
// if the secondary keys of entries are equal
func distinctCompare(entryBytes1, entryBytes2 []byte) bool {
	entry1 := secondaryIndexEntry(entryBytes1)
	entry2 := secondaryIndexEntry(entryBytes2)
	if bytes.Compare(entryBytes1[:entry1.lenKey()], entryBytes2[:entry2.lenKey()]) == 0 {
		return true
	}
	return false
}

func projectKeys(compositekeys [][]byte, key, buf []byte, projection *Projection, cktmp [][]byte) ([]byte, error) {
	var err error

	if projection.entryKeysEmpty {
		entry := secondaryIndexEntry(key)
		buf = append(buf, key[entry.lenKey():]...)
		return buf, nil
	}

	if compositekeys == nil {
		compositekeys, _, err = jsonEncoder.ExplodeArray2(key, buf, nil, cktmp, nil)
		if err != nil {
			return nil, err
		}
	}

	var keysToJoin [][]byte
	for i, projectKey := range projection.projectionKeys {
		if projectKey {
			keysToJoin = append(keysToJoin, compositekeys[i])
		}
	}
	// Note: Reusing the same buf used for Explode in JoinArray as well
	// This is because we always project in order and hence avoiding two
	// different buffers for Explode and Join
	if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
		return nil, err
	}

	entry := secondaryIndexEntry(key)
	buf = append(buf, key[entry.lenKey():]...)
	return buf, nil
}

func projectLeadingKey(compositekeys [][]byte, key []byte, buf *[]byte) ([]byte, error) {
	var err error

	if compositekeys == nil {
		if len(key) > cap(*buf) {
			*buf = make([]byte, 0, len(key)+RESIZE_PAD)
		}
		compositekeys, err = jsonEncoder.ExplodeArray(key, (*buf)[:0])
		if err != nil {
			return nil, err
		}
	}

	var keysToJoin [][]byte
	keysToJoin = append(keysToJoin, compositekeys[0])
	if *buf, err = jsonEncoder.JoinArray(keysToJoin, (*buf)[:0]); err != nil {
		return nil, err
	}

	entry := secondaryIndexEntry(key)
	*buf = append(*buf, key[entry.lenKey():]...)
	return *buf, nil
}

/////////////////////////////////////////////////////////////////////////
//
// group by/aggregate implementation
//
/////////////////////////////////////////////////////////////////////////

type groupKey struct {
	raw       []byte
	obj       value.Value
	n1qlValue bool

	projectId int32
}

type aggrVal struct {
	fn      c.AggrFunc
	raw     []byte
	obj     value.Value
	decoded interface{}

	typ       c.AggrFuncType
	projectId int32
	distinct  bool
	count     int

	n1qlValue bool
}

type aggrRow struct {
	groups []*groupKey
	aggrs  []*aggrVal
	flush  bool
}

type aggrResult struct {
	rows    []*aggrRow
	partial bool
	maxRows int
}

func (g groupKey) String() string {
	if g.n1qlValue {
		return fmt.Sprintf("%v", g.obj)
	} else {
		return fmt.Sprintf("%v", g.raw)
	}
}

func (a aggrVal) String() string {
	return fmt.Sprintf("%v", a.fn)
}

func (a aggrRow) String() string {
	return fmt.Sprintf("group %v aggrs %v flush %v", a.groups, a.aggrs, a.flush)
}

func (a aggrResult) String() string {
	var res string
	for i, r := range a.rows {
		res += fmt.Sprintf("Row %v %v\n", i, r)
	}
	return res
}

func computeGroupAggr(compositekeys, decodedkeys [][]byte, count int, docid, key,
	buf, decbuf []byte, aggrRes *aggrResult, groupAggr *GroupAggr, cktmp, dktmp [][]byte, cachedEntry *entryCache, r *ScanRequest) error {

	var err error

	if groupAggr.IsPrimary {
		compositekeys = make([][]byte, 1)
		compositekeys[0] = key
	} else if compositekeys == nil {
		if groupAggr.NeedExplode {
			if cachedEntry.Exists() {
				if cachedEntry.EqualsEntry(key) {
					compositekeys, decodedkeys = cachedEntry.Get()
					cachedEntry.SetValid(true)
				} else {
					cachedEntry.SetValid(false)
				}
			} else {
				cachedEntry.Init(r, groupAggr.NeedDecode, len(compositekeys))
			}

			if !cachedEntry.Valid() {
				compositekeys, decodedkeys, err = jsonEncoder.ExplodeArray2(key, buf, decbuf, cktmp, dktmp)
				if err != nil {
					return err
				}
				cachedEntry.Update(key, compositekeys, decodedkeys)
			}
		}
	}

	if !cachedEntry.Valid() || groupAggr.DependsOnPrimaryKey {
		for i, gk := range groupAggr.Group {
			err := computeGroupKey(groupAggr, gk, compositekeys, decodedkeys, cachedEntry.decodedvalues, docid, i, r)
			if err != nil {
				return err
			}
		}

		for i, ak := range groupAggr.Aggrs {
			err := computeAggrVal(groupAggr, ak, compositekeys, decodedkeys, cachedEntry.decodedvalues, docid, count, buf, i, r)
			if err != nil {
				return err
			}
		}
	}

	aggrRes.AddNewGroup(groupAggr.groups, groupAggr.aggrs, cachedEntry.Valid())
	return nil

}

func computeGroupKey(groupAggr *GroupAggr, gk *GroupKey, compositekeys,
	decodedkeys [][]byte, decodedvalues []interface{}, docid []byte, pos int, r *ScanRequest) error {

	g := groupAggr.groups[pos]
	if gk.KeyPos >= 0 {
		g.raw = compositekeys[gk.KeyPos]
		g.projectId = gk.EntryKeyId

	} else {
		var scalar value.Value
		if gk.ExprValue != nil {
			scalar = gk.ExprValue // It is a constant expression
		} else {
			var err error
			scalar, err = evaluateN1QLExpresssion(groupAggr, gk.Expr, decodedkeys, decodedvalues, docid, r)
			if err != nil {
				return err
			}
		}

		g.obj = scalar
		g.projectId = gk.EntryKeyId
		g.n1qlValue = true
	}
	return nil
}

func computeAggrVal(groupAggr *GroupAggr, ak *Aggregate,
	compositekeys, decodedkeys [][]byte, decodedvalues []interface{}, docid []byte,
	count int, buf []byte, pos int, r *ScanRequest) error {

	a := groupAggr.aggrs[pos]
	if ak.KeyPos >= 0 {
		if ak.AggrFunc == c.AGG_SUM && !groupAggr.IsPrimary {
			if decodedvalues[ak.KeyPos] == nil {
				actualVal, err := unmarshalValue(decodedkeys[ak.KeyPos])
				if err != nil {
					return err
				}
				decodedvalues[ak.KeyPos] = actualVal
			}
			a.decoded = decodedvalues[ak.KeyPos]
		} else {
			a.raw = compositekeys[ak.KeyPos]
		}

	} else {
		//process expr
		var scalar value.Value
		if ak.ExprValue != nil {
			scalar = ak.ExprValue // It is a constant expression
		} else {
			var err error
			scalar, err = evaluateN1QLExpresssion(groupAggr, ak.Expr, decodedkeys, decodedvalues, docid, r)
			if err != nil {
				return err
			}
		}
		a.obj = scalar
		a.n1qlValue = true
	}

	a.typ = ak.AggrFunc
	a.projectId = ak.EntryKeyId
	a.distinct = ak.Distinct
	a.count = count
	return nil

}

func evaluateN1QLExpresssion(groupAggr *GroupAggr, expr expression.Expression,
	decodedkeys [][]byte, decodedvalues []interface{}, docid []byte, r *ScanRequest) (value.Value, error) {

	if groupAggr.IsPrimary {
		for _, ik := range groupAggr.DependsOnIndexKeys {
			groupAggr.av.SetCover(groupAggr.IndexKeyNames[ik], value.NewValue(string(docid)))
		}
	} else {
		for _, ik := range groupAggr.DependsOnIndexKeys {
			if int(ik) == len(decodedkeys) {
				groupAggr.av.SetCover(groupAggr.IndexKeyNames[ik], value.NewValue(string(docid)))
			} else {
				if decodedvalues[ik] == nil {
					actualVal, err := unmarshalValue(decodedkeys[ik])
					if err != nil {
						return nil, err
					}
					decodedvalues[ik] = actualVal
				}
				groupAggr.av.SetCover(groupAggr.IndexKeyNames[ik], value.NewValue(decodedvalues[ik]))
			}
		}
	}

	t0 := time.Now()
	scalar, _, err := expr.EvaluateForIndex(groupAggr.av, groupAggr.exprContext) // TODO: Ignore vector for now
	if err != nil {
		return nil, err
	}

	if r.Stats != nil {
		r.Stats.Timings.n1qlExpr.Put(time.Since(t0))
	}

	return scalar, nil
}

func (ar *aggrResult) AddNewGroup(groups []*groupKey, aggrs []*aggrVal, cacheValid bool) error {

	var err error

	if cacheValid && len(ar.rows) == 1 {
		err = ar.rows[0].AddAggregate(aggrs)
		if err != nil {
			return err
		}
		return nil
	}

	nomatch := true
	for _, row := range ar.rows {
		if row.CheckEqualGroup(groups) {
			nomatch = false
			err = row.AddAggregate(aggrs)
			if err != nil {
				return err
			}
			break
		}
	}

	if nomatch {
		newRow := &aggrRow{groups: make([]*groupKey, len(groups)),
			aggrs: make([]*aggrVal, len(aggrs))}

		for i, g := range groups {
			if g.n1qlValue {
				newRow.groups[i] = &groupKey{obj: g.obj, projectId: g.projectId, n1qlValue: true}
			} else {
				newKey := make([]byte, len(g.raw))
				copy(newKey, g.raw)
				newRow.groups[i] = &groupKey{raw: newKey, projectId: g.projectId}
			}
		}

		newRow.AddAggregate(aggrs)

		//flush the first row
		if len(ar.rows) >= ar.maxRows {
			ar.rows[0].SetFlush(true)
		}
		ar.rows = append(ar.rows, newRow)
	}

	return nil

}

func (a *aggrResult) SetMaxRows(n int) {
	a.maxRows = n
}

func (ar *aggrRow) CheckEqualGroup(groups []*groupKey) bool {

	for i, gk := range ar.groups {
		if !gk.Equals(groups[i]) {
			return false
		}
	}

	return true
}

func (ar *aggrRow) AddAggregate(aggrs []*aggrVal) error {

	for i, agg := range aggrs {
		if ar.aggrs[i] == nil {
			if agg.n1qlValue {
				ar.aggrs[i] = &aggrVal{fn: c.NewAggrFunc(agg.typ, agg.obj, agg.distinct, true),
					projectId: agg.projectId}
			} else {
				if agg.typ == c.AGG_SUM {
					ar.aggrs[i] = &aggrVal{fn: c.NewAggrFunc(agg.typ, agg.decoded, agg.distinct, false),
						projectId: agg.projectId}
				} else {
					ar.aggrs[i] = &aggrVal{fn: c.NewAggrFunc(agg.typ, agg.raw, agg.distinct, false),
						projectId: agg.projectId}
				}
			}
		} else {
			if agg.n1qlValue {
				ar.aggrs[i].fn.AddDeltaObj(agg.obj)
			} else {
				if agg.typ == c.AGG_SUM {
					ar.aggrs[i].fn.AddDelta(agg.decoded)
				} else {
					ar.aggrs[i].fn.AddDeltaRaw(agg.raw)
				}
			}
		}
		if agg.count > 1 && (agg.typ == c.AGG_SUM || agg.typ == c.AGG_COUNT ||
			agg.typ == c.AGG_COUNTN) {
			for j := 1; j <= agg.count-1; j++ {
				if agg.n1qlValue {
					ar.aggrs[i].fn.AddDeltaObj(agg.obj)
				} else if agg.typ == c.AGG_SUM {
					ar.aggrs[i].fn.AddDelta(agg.decoded)
				} else {
					ar.aggrs[i].fn.AddDeltaRaw(agg.raw)
				}
			}
		}
	}
	return nil
}

func (ar *aggrRow) SetFlush(f bool) {
	ar.flush = f
	return
}

func (ar *aggrRow) Flush() bool {
	return ar.flush
}

func (gk *groupKey) Equals(ok *groupKey) bool {
	if gk.n1qlValue {
		return gk.obj.EquivalentTo(ok.obj)
	} else {
		return bytes.Equal(gk.raw, ok.raw)
	}
}

func projectEmptyResult(buf []byte, projection *Projection, groupAggr *GroupAggr) ([]byte, error) {

	var err error
	//If no group by and no documents qualify, COUNT aggregate
	//should return 0 and all other aggregates should return NULL
	if len(groupAggr.Group) == 0 {

		aggrs := make([][]byte, len(groupAggr.Aggrs))

		for i, ak := range groupAggr.Aggrs {
			if ak.AggrFunc == c.AGG_COUNT || ak.AggrFunc == c.AGG_COUNTN {
				aggrs[i] = encodedZero
			} else {
				aggrs[i] = encodedNull
			}
		}

		var keysToJoin [][]byte
		for _, projGroup := range projection.projectGroupKeys {
			keysToJoin = append(keysToJoin, aggrs[projGroup.pos])
		}

		if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
			l.Errorf("ScanPipeline::projectEmptyResult join array error %v", err)
			return nil, err
		}

		return buf, nil

	} else {
		//If group is not nil and if none of the documents qualify,
		//the aggregate should not return anything
		return nil, nil
	}

	return nil, nil

}

func projectGroupAggr(buf []byte, projection *Projection,
	aggrRes *aggrResult, isPrimary bool) ([]byte, error) {

	var err error
	var row *aggrRow

	for i, r := range aggrRes.rows {
		if r.Flush() {
			row = r
			//TODO - mark the flushed row and discard in one go
			aggrRes.rows = append(aggrRes.rows[:i], aggrRes.rows[i+1:]...)
			break
		}
	}

	if row == nil {
		return nil, nil
	}

	var keysToJoin [][]byte
	for _, projGroup := range projection.projectGroupKeys {
		if projGroup.grpKey {
			gk := row.groups[projGroup.pos]
			if gk.n1qlValue {
				var newKey []byte
				encodeBuf := make([]byte, 1024) // TODO: use val.MarshalJSON() to determine size
				newKey, err = jsonEncoder.EncodeN1QLValue(gk.obj, encodeBuf[:0])
				if err != nil {
					return nil, err
				}
				keysToJoin = append(keysToJoin, newKey)
			} else {
				if isPrimary {
					//TODO: will be optimized as part of overall pipeline optimization
					val, err := encodeValue(string(gk.raw))
					if err != nil {
						l.Errorf("ScanPipeline::projectGroupAggr encodeValue error %v", err)
						return nil, err
					}
					keysToJoin = append(keysToJoin, val)
				} else {
					keysToJoin = append(keysToJoin, gk.raw)
				}
			}
		} else {
			if row.aggrs[projGroup.pos].fn.Type() == c.AGG_SUM ||
				row.aggrs[projGroup.pos].fn.Type() == c.AGG_COUNT ||
				row.aggrs[projGroup.pos].fn.Type() == c.AGG_COUNTN {
				val, err := encodeValue(row.aggrs[projGroup.pos].fn.Value())
				if err != nil {
					l.Errorf("ScanPipeline::projectGroupAggr encodeValue error %v", err)
					return nil, err
				}
				keysToJoin = append(keysToJoin, val)
			} else {
				val := row.aggrs[projGroup.pos].fn.Value()
				switch v := val.(type) {

				case []byte:
					if isPrimary && !isEncodedNull(v) {
						val, err := encodeValue(string(v))
						if err != nil {
							l.Errorf("ScanPipeline::projectGroupAggr encodeValue error %v", err)
							return nil, err
						}
						keysToJoin = append(keysToJoin, val)
					} else {
						keysToJoin = append(keysToJoin, v)
					}

				case value.Value:
					eval, err := encodeValue(v.ActualForIndex())
					if err != nil {
						l.Errorf("ScanPipeline::projectGroupAggr encodeValue error %v", err)
						return nil, err
					}
					keysToJoin = append(keysToJoin, eval)
				}
			}
		}
	}

	if buf, err = jsonEncoder.JoinArray(keysToJoin, buf); err != nil {
		l.Errorf("ScanPipeline::projectGroupAggr join array error %v", err)
		return nil, err
	}

	return buf, nil
}

func unmarshalValue(dec []byte) (interface{}, error) {

	var actualVal interface{}

	//json unmarshal to go type
	err := json.Unmarshal(dec, &actualVal)
	if err != nil {
		return nil, err
	}
	return actualVal, nil
}

func encodeValue(raw interface{}) ([]byte, error) {

	jsonraw, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	encbuf := make([]byte, 3*len(jsonraw)+collatejson.MinBufferSize)
	encval, err := jsonEncoder.Encode(jsonraw, encbuf)
	if err != nil {
		return nil, err
	}

	return encval, nil
}

func isEncodedNull(v []byte) bool {
	return bytes.Equal(v, encodedNull)
}

/////////////////////////////////////////////////////////////////////////
//
// entry cache implementation
//
/////////////////////////////////////////////////////////////////////////

type entryCache struct {
	entry         []byte
	compkeys      [][]byte
	decodedkeys   [][]byte
	decodedvalues []interface{}

	compkeybuf []byte
	deckeybuf  []byte
	valid      bool

	hit  int64
	miss int64
}

func (e *entryCache) Init(r *ScanRequest, needDecode bool, numcompkeys int) {

	entrybuf := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, entrybuf)

	compkeybuf := secKeyBufPool.Get()
	r.keyBufList = append(r.keyBufList, compkeybuf)

	e.entry = (*entrybuf)[:0]
	e.compkeybuf = *compkeybuf

	if needDecode {
		deckeybuf := secKeyBufPool.Get()
		r.keyBufList = append(r.keyBufList, deckeybuf)
		e.deckeybuf = *deckeybuf
	}

}

func (e *entryCache) EqualsEntry(other []byte) bool {
	return distinctCompare(e.entry, other)
}

func (e *entryCache) Get() ([][]byte, [][]byte) {

	return e.compkeys, e.decodedkeys

}

func (e *entryCache) Update(entry []byte, compositekeys [][]byte, decodedkeys [][]byte) {

	e.entry = append(e.entry[:0], entry...)

	if len(entry) > cap(e.compkeybuf) {
		e.compkeybuf = make([]byte, 0, len(entry)+1024)
		if decodedkeys != nil {
			e.deckeybuf = make([]byte, 0, len(entry)+1024)
		}
	}

	if e.compkeys == nil {
		e.compkeys = make([][]byte, len(compositekeys))
	}

	tmpbuf := e.compkeybuf
	for i, k := range compositekeys {
		copy(tmpbuf, k)
		e.compkeys[i] = tmpbuf[:len(k)]
		tmpbuf = tmpbuf[len(k):]
	}

	if decodedkeys != nil {
		if e.decodedkeys == nil {
			e.decodedkeys = make([][]byte, len(decodedkeys))
		}
		tmpbuf := e.deckeybuf
		for i, k := range decodedkeys {
			copy(tmpbuf, k)
			e.decodedkeys[i] = tmpbuf[:len(k)]
			tmpbuf = tmpbuf[len(k):]
		}
	}

	e.decodedvalues = make([]interface{}, len(compositekeys))

}

func (e *entryCache) SetValid(valid bool) {
	if valid {
		e.hit++
	} else {
		e.miss++
	}
	e.valid = valid
}

func (e *entryCache) Exists() bool {
	return e.compkeybuf != nil
}

func (e *entryCache) Valid() bool {
	return e.valid
}

func (e *entryCache) Stats() string {
	return fmt.Sprintf("Hit %v Miss %v", e.hit, e.miss)

}

func (e *entryCache) CacheHitRatio() int {

	if e.hit+e.miss != 0 {
		return int((e.hit * 100) / (e.miss + e.hit))
	} else {
		return 0
	}

}
