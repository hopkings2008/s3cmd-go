package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hopkings2008/s3cmd-go/cmd"
)

// Uploads a file to S3 given a bucket and object key. Also takes a duration
// value to terminate the update if it doesn't complete within that time.
//
// The AWS Region needs to be provided in the AWS shared config or on the
// environment variable as `AWS_REGION`. Credentials also must be provided
// Will default to shared config file, but can load from environment if provided.
//
// Usage:
//   # Upload myfile.txt to myBucket/myKey. Must complete within 10 minutes or will fail
//   go run withContext.go -b mybucket -k myKey -d 10m < myfile.txt
func main() {
	var bucket, endpoint string
	var timeout time.Duration

	iCpuNum := runtime.NumCPU()

	flag.StringVar(&bucket, "b", "", "Bucket name.")
	flag.StringVar(&endpoint, "e", "", "endpoint url.")
	flag.DurationVar(&timeout, "d", 0, "Upload timeout.")
	flag.Parse()

	// All clients require a Session. The Session provides the client with
	// shared configuration such as region, endpoint, and credentials. A
	// Session should be shared where possible to take advantage of
	// configuration and credential caching. See the session package for
	// more information.
	sess := session.Must(session.NewSession())

	// Create a new instance of the service's client with a Session.
	// Optional aws.Config values can also be provided as variadic arguments
	// to the New function. This option allows you to provide service
	// specific configuration.
	svc := s3.New(sess, aws.NewConfig().WithEndpoint(endpoint))

	// Create a context with a timeout that will abort the upload if it takes
	// more than the passed in timeout.
	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}
	// Ensure the context is canceled to prevent leaking.
	// See context package for more information, https://golang.org/pkg/context/
	if cancelFn != nil {
		defer cancelFn()
	}

	listCmd := cmd.NewObjectListCmd(svc)
	listStream := listCmd.ListStream(ctx, bucket)
	var chs []<-chan cmd.ObjectDeleteResult
	for i := 0; i < iCpuNum; i++ {
		deleteCmd := cmd.NewObjectDeleteCmd(svc)
		ch := deleteCmd.DeleteStream(ctx, listStream)
		chs = append(chs, ch)
	}

	result := mergeDeleteResults(ctx, chs...)
	for r := range result {
		if r.Err != nil {
			fmt.Printf("failed to delete %s, err: %v\n", r.Key, r.Err)
			continue
		}
		fmt.Printf("succeeded to delete %s\n", r.Key)
	}

}

func mergeDeleteResults(ctx context.Context, chs ...<-chan cmd.ObjectDeleteResult) <-chan cmd.ObjectDeleteResult {
	var wg sync.WaitGroup
	out := make(chan cmd.ObjectDeleteResult)
	wg.Add(len(chs))

	for _, ch := range chs {
		go func(ctx context.Context, c <-chan cmd.ObjectDeleteResult) {
			defer wg.Done()
			for o := range c {
				select {
				case out <- o:
				case <-ctx.Done():
					return
				}
			}
		}(ctx, ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
