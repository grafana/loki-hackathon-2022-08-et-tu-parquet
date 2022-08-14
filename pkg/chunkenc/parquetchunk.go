package chunkenc

import (
	"context"
	"github.com/grafana/loki/pkg/iter"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql/log"
	"github.com/grafana/loki/pkg/util/filter"
	parquet "github.com/segmentio/parquet-go"
	"io"
	"time"
)

/*
Need to create blocks or an equivalent for reads
Need to write the file from the chunk
*/

type LokiBaseRowType struct {
	timestamp time.Time
	entry     string
}

type parquetChunk struct {
}

func NewParquetChunk(b []byte, blockSize, targetSize int) (*parquetChunk, error) {

	return &parquetChunk{}, nil
}

func (parquetChunk) Bounds() (time.Time, time.Time) {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) SpaceFor(l *logproto.Entry) bool {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Append(l *logproto.Entry) error {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Iterator(ctx context.Context, mintT, maxtT time.Time, direction logproto.Direction, pipeline log.StreamPipeline) (iter.EntryIterator, error) {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) SampleIterator(ctx context.Context, from, through time.Time, extractor log.StreamSampleExtractor) iter.SampleIterator {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Blocks(mintT, maxtT time.Time) []Block {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Size() int {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Bytes() ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) BytesWith(bytes []byte) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) WriteTo(w io.Writer) (n int64, err error) {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) BlockCount() int {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Utilization() float64 {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) UncompressedSize() int {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) CompressedSize() int {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Close() error {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Encoding() Encoding {
	//TODO implement me
	panic("implement me")
}

func (parquetChunk) Rebound(start, end time.Time, filter filter.Func) (Chunk, error) {
	//TODO implement me
	panic("implement me")
}

func writeParquet(w io.Writer, c Chunk) error {

	// Define the schema
	// Metadata
	// - a version number of the schema
	// - labels for the stream
	// - block data?
	vers := parquet.KeyValueMetadata("version", "1")
	lbls := parquet.KeyValueMetadata("labels", "FIXME")
	pw := parquet.NewGenericWriter[LokiBaseRowType](w, vers, lbls)
	defer pw.Close()

	// Write the blocks
	blocks := c.Blocks(c.Bounds())
	for _, b := range blocks {
		itr := b.Iterator(context.Background(), nil)
		for itr.Next() {
			pw.Write([]LokiBaseRowType{
				{
					timestamp: itr.Entry().Timestamp,
					entry:     itr.Entry().Line,
				},
			})
		}
		// Calling flush will create a row group, we are mapping a block to a row group
		pw.Flush()
	}

	return nil

}
