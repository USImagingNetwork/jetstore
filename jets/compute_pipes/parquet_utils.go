package compute_pipes

import (
	"fmt"
	"os"

	goparquet "github.com/fraugster/parquet-go"
)

// Utility function for reading parquet files

func GetRawHeadersParquet(fileName string) (*[]string, error) {
	// Get rawHeaders
	var fileHd *os.File
	var err error
	fileHd, err = os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("error opening temp file: %v", err)
	}
	defer fileHd.Close()
		// Get the file headers from the parquet schema
		parquetReader, err := goparquet.NewFileReader(fileHd)
		if err != nil {
			return nil, err
		}
		rawHeaders, err := getParquetFileHeaders(parquetReader)
		if err != nil {
			return nil, fmt.Errorf("while reading parquet headers: %v", err)
		}
		// Make sure we don't have empty names in rawHeaders
		AdjustFillers(rawHeaders)
		fmt.Println("Got input columns (rawHeaders) from parquet file:", rawHeaders)
		return rawHeaders, nil
}

func getParquetFileHeaders(parquetReader *goparquet.FileReader) (*[]string, error) {
	rawHeaders := make([]string, 0)
	sd := parquetReader.GetSchemaDefinition()
	for i := range sd.RootColumn.Children {
		cd := sd.RootColumn.Children[i]
		rawHeaders = append(rawHeaders, cd.SchemaElement.Name)
	}
	return &rawHeaders, nil
}