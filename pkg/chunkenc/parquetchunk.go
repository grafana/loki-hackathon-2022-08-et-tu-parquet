package chunkenc

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"github.com/grafana/loki/pkg/iter"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql/log"
	"github.com/grafana/loki/pkg/logqlmodel/stats"
	"github.com/grafana/loki/pkg/storage/chunk"
	"github.com/grafana/loki/pkg/util/filter"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"io"
	"math"
	"reflect"
	"strconv"
	"time"
)

type LokiBaseRowType struct {
	Timestamp int64             `parquet:",delta"`
	Entry     string            `parquet:",snappy"`
	Metadata  map[string]string `parquet: ",snappy"`
}

type ParquetChunk struct {
	parquetFile *parquet.File
	blocks      []parquetBlock
	labels      labels.Labels
}

func NewParquetChunk(b []byte) (*ParquetChunk, error) {
	pf, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return nil, err
	}

	pc := &ParquetChunk{
		parquetFile: pf,
	}
	pc.blocks = make([]parquetBlock, 0, len(pf.RowGroups()))
	for i, rg := range pf.RowGroups() {
		tsColumn := rg.ColumnChunks()[0]
		minTime := int64(math.MaxInt64)
		maxTime := int64(math.MinInt64)
		for j := 0; j < tsColumn.ColumnIndex().NumPages(); j++ {
			min := tsColumn.ColumnIndex().MinValue(j).Int64()
			if min < minTime {
				minTime = min
			}
			max := tsColumn.ColumnIndex().MaxValue(j).Int64()
			if max > maxTime {
				maxTime = max
			}
		}

		pb := parquetBlock{
			minTime:  minTime,
			maxTime:  maxTime,
			offset:   i,
			rowGroup: pf.RowGroups()[i],
		}
		pc.blocks = append(pc.blocks, pb)
	}

	if s, ok := pf.Lookup("labels"); ok {
		pc.labels = labels.Labels{}
		err := pc.labels.UnmarshalJSON([]byte(s))
		if err != nil {
			return nil, err
		}
	}

	return pc, nil
}

func writeParquet(w io.Writer, c Chunk, labels labels.Labels) error {

	// Define the schema
	// Metadata
	// - a version number of the schema
	// - labels for the stream
	// - block data?
	vers := parquet.KeyValueMetadata("version", "1")
	lb, err := labels.MarshalJSON()
	if err != nil {
		return err
	}
	lbls := parquet.KeyValueMetadata("labels", string(lb))
	uncompressedSize := parquet.KeyValueMetadata("uncompressed_size", fmt.Sprintf("%d", c.UncompressedSize()))
	st, et := c.Bounds()
	startTime := parquet.KeyValueMetadata("start_time", fmt.Sprintf("%d", st.UnixNano()))
	endTime := parquet.KeyValueMetadata("end_time", fmt.Sprintf("%d", et.UnixNano()))

	fields := []reflect.StructField{
		{
			Name: "Timestamp",
			Type: reflect.TypeOf(int64(0)),
			Tag:  `parquet:",delta"`,
		},
		{
			Name: "Line",
			Type: reflect.TypeOf(""),
			Tag:  `parquet:",snappy"`,
		},
	}
	mc := c.MetadataColumns()
	for k, v := range mc {
		fields = append(fields, reflect.StructField{Name: k, Type: v, Tag: `parquet:",snappy"`})
	}
	typ := reflect.StructOf(fields)
	//schema := parquet.PissSchemaOf(typ)
	schema := parquet.SchemaOf(reflect.New(typ).Interface())
	pw := parquet.NewWriter(w, schema, vers, lbls, uncompressedSize, startTime, endTime)
	defer pw.Close()

	// Write the blocks
	blocks := c.Blocks(c.Bounds())
	for _, b := range blocks {
		itr := b.Iterator(context.Background(), noopStreamPipeline)
		for itr.Next() {
			row := reflect.New(typ)
			//derefRow := dereference(row)
			row.Elem().FieldByName("Timestamp").Set(reflect.ValueOf(itr.Entry().Timestamp.UnixNano()))
			row.Elem().FieldByName("Line").Set(reflect.ValueOf(itr.Entry().Line))

			for c := range c.MetadataColumns() {
				// TODO check types here
				// TODO check that the column exists in the metadata
				row.Elem().FieldByName(c).Set(reflect.ValueOf(itr.Entry().Metadata[c]))
			}

			//row := make(parquet.Row, 2)
			//row[0] = parquet.ValueOf(itr.Entry().Timestamp.UnixNano())
			//row[1] = parquet.ValueOf(itr.Entry().Line)
			//row[0] = row[0].Level(0, 0, 0)
			//row[1] = row[1].Level(0, 0, 1)
			// TODO write more than one row at a time?
			//fmt.Printf("%v", row)
			err := pw.Write(row.Interface())
			if err != nil {
				return err
			}
		}
		err := itr.Close()
		if err != nil {
			return err
		}
		// Calling flush will create a row group, we are mapping a block to a row group
		err = pw.Flush()
		if err != nil {
			return err
		}
	}

	return nil

}

func (p ParquetChunk) Labels() labels.Labels {
	return p.labels
}

// Add implements chunk.Data
func (p ParquetChunk) Add(sample model.SamplePair) (chunk.Data, error) {
	//TODO implement me
	panic("implement me")
}

// Marshal implements chunk.Data
func (p ParquetChunk) Marshal(writer io.Writer) error {
	//TODO implement me
	panic("implement me")
}

// UnmarshalFromBuf implements chunk.Data
func (p ParquetChunk) UnmarshalFromBuf(i []byte) error {
	//TODO implement me
	panic("implement me")
}

// Entries implements chunk.Data
func (p ParquetChunk) Entries() int {
	//TODO implement me
	panic("implement me")
}

func (p ParquetChunk) Bounds() (time.Time, time.Time) {
	// TODO we can get this from the blocks I believe and not have to save these in metadata
	var start, end time.Time
	if s, ok := p.parquetFile.Lookup("start_time"); ok {
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			//FIXME log error
			goto theHardWay
		}
		start = time.Unix(0, i)
	}

	if s, ok := p.parquetFile.Lookup("end_time"); ok {
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			//FIXME log error
			goto theHardWay
		}
		end = time.Unix(0, i)
	}

	if !start.IsZero() && !end.IsZero() {
		return start, end
	}

	// TODO how do we handle this? if we never release a version that doesn't have the required metadata do we even need this?
theHardWay:
	panic("implement me")
}

func (ParquetChunk) SpaceFor(l *logproto.Entry) bool {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) Append(l *logproto.Entry) error {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) Iterator(ctx context.Context, mintT, maxtT time.Time, direction logproto.Direction, pipeline log.StreamPipeline) (iter.EntryIterator, error) {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) SampleIterator(ctx context.Context, from, through time.Time, extractor log.StreamSampleExtractor) iter.SampleIterator {
	//TODO implement me
	panic("implement me")
}

func (p *ParquetChunk) Blocks(mintT, maxtT time.Time) []Block {
	mint, maxt := mintT.UnixNano(), maxtT.UnixNano()
	blocks := make([]Block, 0, len(p.blocks))

	for _, b := range p.blocks {
		if maxt >= b.minTime && b.maxTime >= mint {
			blocks = append(blocks, b)
		}
	}
	return blocks
}

func (p ParquetChunk) Size() int {
	return int(p.parquetFile.NumRows())
}

func (ParquetChunk) Bytes() ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) BytesWith(bytes []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) WriteTo(w io.Writer) (n int64, err error) {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) BlockCount() int {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) Utilization() float64 {
	//TODO implement me
	panic("implement me")
}

func (p ParquetChunk) UncompressedSize() int {
	if s, ok := p.parquetFile.Lookup("uncompressed_size"); ok {
		i, err := strconv.Atoi(s)
		if err != nil {
			//FIXME log error
			return -1
		}
		return i
	}
	//FIXME log error?
	return -1
}

func (p ParquetChunk) CompressedSize() int {
	return int(p.parquetFile.Size())
}

func (ParquetChunk) Close() error {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) Encoding() Encoding {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) Rebound(start, end time.Time, filter filter.Func) (Chunk, error) {
	//TODO implement me
	panic("implement me")
}

func (ParquetChunk) MetadataColumns() map[string]reflect.Type {
	panic("who would call this?")
}

type parquetBlock struct {
	minTime  int64
	maxTime  int64
	offset   int
	rowGroup parquet.RowGroup
}

func (p parquetBlock) MinTime() int64 {
	return p.minTime
}

func (p parquetBlock) MaxTime() int64 {
	return p.maxTime
}

func (p parquetBlock) Offset() int {
	return p.offset
}

func (p parquetBlock) Entries() int {
	return int(p.rowGroup.NumRows())
}

func (p parquetBlock) Iterator(ctx context.Context, pipeline log.StreamPipeline) iter.EntryIterator {
	return newparquetEntryIterator(ctx, pipeline, &p)
}

func (p parquetBlock) SampleIterator(ctx context.Context, extractor log.StreamSampleExtractor) iter.SampleIterator {
	return newparquetSampleIterator(ctx, extractor, &p)
}

type parquetSampleIterator struct {
	extractor log.StreamSampleExtractor
	stats     *stats.Context
	reader    *parquet.GenericReader[LokiBaseRowType]

	cur        logproto.Sample
	currLabels log.LabelsResult

	err    error
	closed bool
}

func newparquetSampleIterator(ctx context.Context, extractor log.StreamSampleExtractor, block *parquetBlock) iter.SampleIterator {

	return &parquetSampleIterator{
		stats:     stats.FromContext(ctx),
		extractor: extractor,
		reader:    parquet.NewGenericRowGroupReader[LokiBaseRowType](block.rowGroup),
	}
}

func (p *parquetSampleIterator) Next() bool {
	if p.closed {
		return false
	}
	for {
		//TODO we should read in batches here instead of one at a time I think.
		row := make([]LokiBaseRowType, 1, 1)
		num, err := p.reader.Read(row)
		if num > 0 {
			//TODO memchunk adds additional length to cover the timestamp, not sure what to add here for parquet files
			p.stats.AddDecompressedBytes(int64(len(row[0].Entry)))
			p.stats.AddDecompressedLines(1)

			currLine := []byte(row[0].Entry)
			currTs := row[0].Timestamp

			val, labels, ok := p.extractor.Process(currTs, currLine)
			if !ok {
				continue
			}
			p.currLabels = labels
			p.cur.Value = val
			p.cur.Hash = xxhash.Sum64(currLine)
			p.cur.Timestamp = currTs

			return true
		}
		if err == io.EOF {
			p.closed = true
			p.reader.Close()
			return false
		}
	}
}

func (p *parquetSampleIterator) Labels() string {
	return p.currLabels.String()
}

func (p *parquetSampleIterator) StreamHash() uint64 {
	return p.extractor.BaseLabels().Hash()
}

func (p *parquetSampleIterator) Error() error {
	return p.err
}

func (p *parquetSampleIterator) Close() error {
	if !p.closed {
		p.closed = true
	}
	return p.err
}

func (p *parquetSampleIterator) Sample() logproto.Sample {
	return p.cur
}

type parquetEntryIterator struct {
	pipeline log.StreamPipeline
	stats    *stats.Context
	reader   *parquet.Reader

	cur        logproto.Entry
	currLabels log.LabelsResult

	err    error
	closed bool
}

func newparquetEntryIterator(ctx context.Context, pipeline log.StreamPipeline, block *parquetBlock) iter.EntryIterator {

	return &parquetEntryIterator{
		stats:    stats.FromContext(ctx),
		pipeline: pipeline,
		reader:   parquet.NewRowGroupReader(block.rowGroup),
	}
}

func (p *parquetEntryIterator) Next() bool {
	if p.closed {
		return false
	}
	for {
		//TODO we should read in batches here instead of one at a time I think.
		//row := make([]LokiBaseRowType, 1, 1)
		row := make([]parquet.Row, 1, 1)
		num, err := p.reader.ReadRows(row)
		//num, err := p.reader.Read(row)
		if num > 0 {
			//TODO memchunk adds additional length to cover the timestamp, not sure what to add here for parquet files
			p.stats.AddDecompressedBytes(int64(len(row[0][1].Bytes())))
			p.stats.AddDecompressedLines(1)

			columns := p.reader.Schema().Columns()
			fmt.Println(columns)

			newLine, lbs, matches := p.pipeline.Process(row[0][0].Int64(), row[0][1].Bytes())
			if !matches {
				continue
			}
			p.cur.Timestamp = time.Unix(0, row[0][0].Int64())
			p.cur.Line = string(newLine)
			p.currLabels = lbs
			return true
		}
		if err == io.EOF {
			p.closed = true
			p.reader.Close()
			return false
		}
	}
}

func (p *parquetEntryIterator) Labels() string {
	return p.currLabels.String()
}

func (p *parquetEntryIterator) StreamHash() uint64 {
	return p.pipeline.BaseLabels().Hash()
}

func (p *parquetEntryIterator) Error() error {
	return p.err
}

func (p *parquetEntryIterator) Close() error {
	if !p.closed {
		p.closed = true
	}
	return p.err
}

func (p *parquetEntryIterator) Entry() logproto.Entry {
	return p.cur
}

func dereference(t reflect.Value) reflect.Value {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}
