package cmd

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type ObjectElem struct {
	Bucket string
	Key    string
}

type ObjectListCmd struct {
	s3  *s3.S3
	err error
}

func (olc *ObjectListCmd) GetLastError() error {
	return olc.err
}

func (olc *ObjectListCmd) ListStream(ctx context.Context, bucket string) <-chan ObjectElem {
	out := make(chan ObjectElem)

	go func(ctx context.Context) {
		defer close(out)
		err := olc.s3.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
			Bucket: aws.String(bucket),
		}, func(p *s3.ListObjectsOutput, lastPage bool) bool {
			for _, o := range p.Contents {
				oe := ObjectElem{
					Bucket: bucket,
					Key:    *o.Key,
				}
				select {
				case out <- oe:
				case <-ctx.Done():
					return false
				}
			}
			return true // continue paging
		})
		if err != nil {
			fmt.Printf("failed to list objects for bucket, %s, %v\n", bucket, err)
			olc.err = err
		}
	}(ctx)

	return out
}

func NewObjectListCmd(s3 *s3.S3) *ObjectListCmd {
	return &ObjectListCmd{
		s3: s3,
	}
}
