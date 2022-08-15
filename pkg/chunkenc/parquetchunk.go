package chunkenc

import (
	"context"
	"github.com/grafana/loki/pkg/iter"
	"github.com/grafana/loki/pkg/logproto"
	"github.com/grafana/loki/pkg/logql/log"
	"github.com/grafana/loki/pkg/util/filter"
	"github.com/prometheus/prometheus/model/labels"
	parquet "github.com/segmentio/parquet-go"
	"io"
	"time"
)

/*
Need to create blocks or an equivalent for reads
Need to write the file from the chunk
*/

type LokiBaseRowType struct {
	Timestamp int64  `parquet:",delta"`
	Entry     string `parquet:",snappy"`
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

func writeParquet(w io.Writer, c Chunk, labels labels.Labels) error {

	// Define the schema
	// Metadata
	// - a version number of the schema
	// - labels for the stream
	// - block data?
	vers := parquet.KeyValueMetadata("version", "1")
	lbls := parquet.KeyValueMetadata("labels", labels.String())
	schema := parquet.SchemaOf(new(LokiBaseRowType))
	pw := parquet.NewGenericWriter[LokiBaseRowType](w, schema, vers, lbls)
	defer pw.Close()

	// Write the blocks
	blocks := c.Blocks(c.Bounds())
	for _, b := range blocks {
		itr := b.Iterator(context.Background(), noopStreamPipeline)
		//TODO This should batch write instead of doing one entry at a time.
		for itr.Next() {
			_, err := pw.Write([]LokiBaseRowType{
				{
					Timestamp: itr.Entry().Timestamp.UnixNano(),
					Entry:     itr.Entry().Line,
				},
			})
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
