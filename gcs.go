package treeply

import (
	"context"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type GCSRemoteProvider struct {
	client *storage.Client
	root   string
}

func NewGCSRemoteProvider(ctx context.Context, root string) *GCSRemoteProvider {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return &GCSRemoteProvider{client: client, root: root}
}

var GCSPathRegEx = regexp.MustCompile("gs://([^/]+)/(.*)$")

func parseGCSPath(path string) (string, string, error) {
	matches := GCSPathRegEx.FindStringSubmatch(path)
	if matches == nil {
		return "", "", fmt.Errorf("Invalid gcs path")
	}
	bucket := matches[1]
	key := matches[2]
	return bucket, key, nil
}

func (g *GCSRemoteProvider) GetDirListing(ctx context.Context, path string) ([]RemoteFile, error) {
	bucketName, key, err := parseGCSPath(pathConcat(g.root, path))
	if err != nil {
		return nil, err
	}

	bucket := g.client.Bucket(bucketName)
	var prefix string
	if key == "" {
		prefix = ""
	} else {
		prefix = key + "/"
	}
	log.Printf("listing objects in %s, with prefix %s (root=%s, path=%s)", bucketName, prefix, g.root, path)
	objIt := bucket.Objects(ctx, &storage.Query{Delimiter: "/", Prefix: prefix, Projection: storage.ProjectionNoACL})

	result := make([]RemoteFile, 0, 100)
	for {
		objAttr, err := objIt.Next()
		if objAttr != nil {
			log.Printf("objAttr name: %s, prefix: %s, size: %d", objAttr.Name, objAttr.Prefix, objAttr.Size)
			var name string
			var isDir bool

			if objAttr.Name != "" {
				name = objAttr.Name
				isDir = false
			} else {
				name = objAttr.Prefix[:len(objAttr.Prefix)-1]
				log.Printf("setting name to %s based on prefix %s", name, objAttr.Prefix)
				isDir = true
			}

			name = name[len(prefix):]

			result = append(result, RemoteFile{
				Name:  name,
				IsDir: isDir,
				ETag:  fmt.Sprintf("%d", objAttr.Generation),
				Size:  objAttr.Size,
			})
		}
		if err == iterator.Done {
			log.Printf("Reached end of iterator")
			break
		}
	}
	return result, nil
}
func (g *GCSRemoteProvider) GetReader(ctx context.Context, path string, ETag string, Offset int64, Length int64) (io.Reader, error) {
	generationID, err := strconv.ParseInt(ETag, 10, 64)
	if err != nil {
		panic("Could not convert etag to int")
	}

	bucketName, key, err := parseGCSPath(pathConcat(g.root, path))
	if err != nil {
		return nil, err
	}
	bucket := g.client.Bucket(bucketName)
	obj := bucket.Object(key)
	reader, err := obj.If(storage.Conditions{GenerationMatch: generationID}).NewRangeReader(ctx, Offset, Length)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

func (g *GCSRemoteProvider) GetDiagnostics() interface{} {
	return nil
}

// banana banana hedgehog

// /`-.__                                  /\
//  `. .  ~~--..__                   __..-' ,'
//    `.`-.._     ~~---...___...---~~  _,~,/
// 	   `-._ ~--..__             __..-~ _-~
// 	    	~~-..__ ~~--.....--~~   _.-~
// 		    	   ~~--...___...--~~
// /`-.__                                  /\
//  `. .  ~~--..__                   __..-' ,'
//    `.`-.._     ~~---...___...---~~  _,~,/
// 	   `-._ ~--..__             __..-~ _-~
// 	    	~~-..__ ~~--.....--~~   _.-~
// 		    	   ~~--...___...--~~
//
//   .::::::::..          ..::::::::.
//  :::::::::::::        :::::::::::::
// :::::::::::' .\      /. `:::::::::::
// `::::::::::_,__o    o__,_::::::::::'
