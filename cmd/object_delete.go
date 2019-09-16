package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go/service/s3"
)

type ObjectDeleteCmd struct {
	s3 *s3.S3
}

type ObjectDeleteResult struct {
	Bucket string
	Key    string
	Err    error
}

func (odc *ObjectDeleteCmd) DeleteStream(ctx context.Context, in <-chan ObjectElem) <-chan ObjectDeleteResult {
	out := make(chan ObjectDeleteResult)
	go func(ctx context.Context) {
		defer close(out)
		for o := range in {
			_, err := odc.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
				Bucket: &o.Bucket,
				Key:    &o.Key,
			})
			odr := ObjectDeleteResult{
				Bucket: o.Bucket,
				Key:    o.Key,
			}
			if err != nil {
				odr.Err = err
			}
			select {
			case out <- odr:
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
	return out
}

func NewObjectDeleteCmd(s3 *s3.S3) *ObjectDeleteCmd {
	return &ObjectDeleteCmd{
		s3: s3,
	}
}
