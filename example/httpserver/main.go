package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stind/s3zip"
)

type Request struct {
	Resources []Resource `json:"resources"`
	Object    Object     `json:"object"`
}

type Resource struct {
	Object   Object `json:"object"`
	FileName string `json:"file_name"`
}

type Object struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type handler struct {
	svc *s3.S3
	z   s3zip.S3Zip
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	r = r.WithContext(ctx)

	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()

	var req Request
	err := decoder.Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "failed to decode json: %v", err)
		return
	}

	resources := make([]s3zip.Resource, len(req.Resources))
	for i, res := range req.Resources {
		resources[i] = s3zip.Resource{
			FileName: res.FileName,
			Object:   s3zip.Object(res.Object),
		}
	}
	err = h.z.Do(ctx, s3zip.Object(req.Object), resources)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, err.Error())
		return
	}

	presignReq, _ := h.svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(req.Object.Bucket),
		Key:    aws.String(req.Object.Key),
	})
	urlStr, err := presignReq.Presign(15 * time.Minute)

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, urlStr)
}

func main() {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	z := s3zip.New(sess, s3zip.WithConcurrency(10))
	h := handler{svc: svc, z: z}

	srv := http.Server{
		Addr:         "localhost:8080",
		ReadTimeout:  90 * time.Second,
		WriteTimeout: 90 * time.Second,
		IdleTimeout:  120 * time.Second,
		Handler:      h,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("failed to start a server: %v", err)
	}
}
