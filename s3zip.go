// Package s3zip provides support for compressing AWS S3 objects.
package s3zip

import awsclient "github.com/aws/aws-sdk-go/aws/client"

type S3Zip struct {
	cfg         awsclient.ConfigProvider
	concurrency int
}

type configOption func(*S3Zip)

// New creates a new S3Zip instance
func New(cfg awsclient.ConfigProvider, opts ...configOption) S3Zip {
	const defaultConcurrency = 1

	z := S3Zip{
		cfg:         cfg,
		concurrency: defaultConcurrency,
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
