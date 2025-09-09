package compute_pipes

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3DeviceManager manages a pool of S3DeviceWorker to put local files
// to s3

// S3DeviceManager manage a pool of workers to put file to s3.
// ClientWg is a wait group of the partition writers created during
// BuildComputeGraph function. The WorkersTaskCh is closed in process_file.go
type S3DeviceManager struct {
	cpConfig                 *ComputePipesConfig
	s3WorkerPoolSize         int
	WorkersTaskCh            chan S3Object
	ClientsWg                *sync.WaitGroup
	ParticipatingTempFolders []string
}

// S3Object is the worker's task payload to put a file to s3
type S3Object struct {
	ExternalBucket string
	FileKey        string
	LocalFilePath  string
}

// Create the S3DeviceManager
func (cpCtx *ComputePipesContext) NewS3DeviceManager() error {
	// log.Println("Entering NewS3DeviceManager")
	if cpCtx.CpConfig.ClusterConfig.S3WorkerPoolSize < 1 {
		return fmt.Errorf("error: S3DeviceManager cannot have s3_worker_pool_size < 1")
	}
	// Create the s3 uploader that will be used by all the workers
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(regionName))
	if err != nil {
		return fmt.Errorf("while loading aws configuration (in NewS3DeviceManager): %v", err)
	}
	// Create the uploader
	s3Uploader := manager.NewUploader(s3.NewFromConfig(cfg))

	// Create the s3 device manager
	var clientsWg sync.WaitGroup
	s3DeviceManager := &S3DeviceManager{
		cpConfig:                 cpCtx.CpConfig,
		s3WorkerPoolSize:         cpCtx.CpConfig.ClusterConfig.S3WorkerPoolSize,
		WorkersTaskCh:            make(chan S3Object, 10),
		ParticipatingTempFolders: make([]string, 0),
		ClientsWg:                &clientsWg,
	}

	// Create a channel for the workers to report results
	s3WorkersResultCh := make(chan ComputePipesResult)
	// Collect the results from all the workers
	go func() {
		var partCount int64
		var err error
		for workerResult := range s3WorkersResultCh {
			partCount += workerResult.PartsCount
			if workerResult.Err != nil {
				err = workerResult.Err
				break
			}
		}
		// Send out the collected result
		select {
		case cpCtx.ChResults.S3PutObjectResultCh <- ComputePipesResult{
			PartsCount: partCount,
			Err:        err}:
			if err != nil {
				// Interrupt the whole process, there's been an error writing a file part
				// Avoid closing a closed channel
				select {
				case <-cpCtx.Done:
				default:
					close(cpCtx.Done)
				}
			}
		case <-cpCtx.Done:
			log.Printf("Collecting results from S3DeviceWorker interrupted")
		}
		close(cpCtx.ChResults.S3PutObjectResultCh)
	}()

	// Set up all the workers, use a wait group to track when they are all done
	// to close s3WorkersResultCh
	log.Printf("NewS3DeviceManager: Creating %d s3 workers", s3DeviceManager.s3WorkerPoolSize)
	go func() {
		var wg sync.WaitGroup
		for range s3DeviceManager.s3WorkerPoolSize {
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker := NewS3DeviceWorker(s3Uploader, cpCtx.Done, cpCtx.ErrCh)
				worker.DoWork(s3DeviceManager, s3WorkersResultCh)
			}()
		}
		wg.Wait()
		close(s3WorkersResultCh)
		// Cleaned up all participating temp folders
		for _, folderPath := range s3DeviceManager.ParticipatingTempFolders {
			err := os.RemoveAll(folderPath)
			if err != nil {
				log.Printf("%s - WARNING while calling RemoveAll for s3 Device Manager:%v", cpCtx.SessionId, err)
			}
		}
	}()
	// Set the S3DeviceManager to ComputePipesContext so it's avail when cpipes wind down
	cpCtx.S3DeviceMgr = s3DeviceManager
	return nil
}
