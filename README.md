# s3zip

Package s3zip provides support for compressing AWS S3 objects.

It's a Proof of Concept project that hasn't been used in production.

## Usage

```go
package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/stind/s3zip"
)

func main() {
	sess := session.Must(session.NewSession())
	z := s3zip.New(sess, s3zip.WithConcurrency(10))
	ctx := context.Background()

	// Where to upload the zip archive
	destination := s3Zip.Object{
		Bucket: "destination-bucket-name",
		Key:    "destination-archive-name.zip",
	}

	resources := []s3zip.Resource{
		{
			FileName: "foo.txt",
			Object: s3zip.Object{
				Bucket: "source-bucket-name",
				Key:    "key/of/source-file-name.txt",
			},
		},
		{
			FileName: "bar/baz.txt",
			Object: s3zip.Object{
				Bucket: "source-bucket-name",
				Key:    "other/key/of/source-file-name.txt",
			},
		},
	}

	err := z.Do(ctx, destination, resources...)
	if err != nil {
		log.Fatal(err)
	}
}
```
