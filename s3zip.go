// Package s3zip provides support for compressing AWS S3 objects.
package s3zip

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	awsclient "github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Zip struct {
	concurrency int
	uploader    *s3manager.Uploader
	downloader  *s3manager.Downloader
}

type configOption func(*S3Zip)

// New creates a new S3Zip instance
func New(c awsclient.ConfigProvider, opts ...configOption) S3Zip {
	const defaultConcurrency = 1

	downloader := s3manager.NewDownloader(c, func(d *s3manager.Downloader) {
		d.Concurrency = 1
	})

	z := S3Zip{
		concurrency: defaultConcurrency,
		uploader:    s3manager.NewUploader(c),
		downloader:  downloader,
	}

	for _, opt := range opts {
		opt(&z)
	}

	return z
}

func WithConcurrency(c int) configOption {
	return func(z *S3Zip) {
		z.concurrency = c
	}
}

// Resource describes an S3 object that has to be packed into a zip archive.
type Resource struct {
	Object   Object
	FileName string // FileName is a desired path to the file in the archive.

	// Path to the downloaded file on disk. Must be removed when no more needed.
	fpath string
}

// Object describes an S3 object.
type Object struct {
	Bucket string
	Key    string
}

// Do downloads S3 objects, puts them into a zip archive
// and uploads it to the destination S3 object.
func (z S3Zip) Do(ctx context.Context, destObj Object, resources []Resource) error {
	// Start workers
	workerQueue := gen(resources...)
	workerChannels := make([]<-chan Resource, z.concurrency)
	for i := 0; i < z.concurrency; i++ {
		workerChannels[i] = z.runDownloadWorker(ctx, workerQueue)
	}

	// Zip downloaded files
	zipFpath, err := archive(merge(workerChannels...))
	if err != nil {
		return fmt.Errorf("failed to zip: %w", err)
	}
	defer os.Remove(zipFpath)

	// Upload zip to S3
	err = z.upload(ctx, destObj, zipFpath)
	if err != nil {
		return fmt.Errorf("failed to upload zip: %w", err)
	}

	return nil
}

func (z S3Zip) runDownloadWorker(ctx context.Context, queue <-chan Resource) <-chan Resource {
	out := make(chan Resource)

	go func() {
		defer close(out)

		for res := range queue {
			res, err := z.downloadOnDisk(ctx, res)
			if err != nil {
				return
			}

			out <- res
		}
	}()

	return out
}

func (z S3Zip) downloadOnDisk(ctx context.Context, res Resource) (Resource, error) {
	// Create file to download into
	f, err := os.CreateTemp("", "*."+res.FileName)
	if err != nil {
		return res, err
	}
	defer f.Close()

	err = z.download(ctx, f, res.Object)
	if err != nil {
		return res, fmt.Errorf("failed to download: %w", err)
	}

	res.fpath = f.Name()

	return res, nil
}

func (z S3Zip) download(ctx context.Context, w io.WriterAt, obj Object) error {
	_, err := z.downloader.DownloadWithContext(ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(obj.Bucket),
		Key:    aws.String(obj.Key),
	})

	return err
}

func (z S3Zip) upload(ctx context.Context, destObj Object, fpath string) error {
	zf, err := os.Open(fpath)
	if err != nil {
		return fmt.Errorf("failed to open a file: %w", err)
	}
	defer zf.Close()

	zipInput := s3manager.UploadInput{
		Bucket: aws.String(destObj.Bucket),
		Key:    aws.String(destObj.Key),
		Body:   zf,
	}
	_, err = z.uploader.UploadWithContext(ctx, &zipInput)

	return err
}

// archive returns a path to zip file with items from the queue.
func archive(queue <-chan Resource) (string, error) {
	zf, err := os.CreateTemp("", "*.s3.zip")
	if err != nil {
		return "", fmt.Errorf("failed to create a temp zip file: %w", err)
	}
	defer zf.Close()

	zw := zip.NewWriter(zf)
	for res := range queue {
		if res.fpath == "" {
			continue
		}
		defer os.Remove(res.fpath)

		err := addToZip(zw, res)
		if err != nil {
			return "", err
		}
	}

	err = zw.Close()
	if err != nil {
		return "", fmt.Errorf("failed to close zip file: %w", err)
	}

	return zf.Name(), nil
}

func addToZip(zw *zip.Writer, res Resource) error {
	w, err := zw.Create(res.FileName)
	if err != nil {
		return fmt.Errorf("failed to add a file to the zip file: %w", err)
	}

	resFile, err := os.Open(res.fpath)
	if err != nil {
		return fmt.Errorf("failed to open a downloaded file: %w", err)
	}
	defer resFile.Close()

	_, err = io.Copy(w, resFile)
	if err != nil {
		return fmt.Errorf("failed to compress a file: %w", err)
	}

	return nil
}

func gen(resources ...Resource) <-chan Resource {
	ch := make(chan Resource)

	go func() {
		defer close(ch)

		for _, res := range resources {
			ch <- res
		}
	}()

	return ch
}

func merge(cs ...<-chan Resource) <-chan Resource {
	var wg sync.WaitGroup
	out := make(chan Resource)

	wg.Add(len(cs))

	for _, c := range cs {
		go func(c <-chan Resource) {
			for r := range c {
				out <- r
			}
			wg.Done()
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
