package treeply

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGCS(t *testing.T) {
	ctx := context.Background()
	gcs := NewGCSRemoteProvider(ctx, "gs://")
	files, err := gcs.GetDirListing(ctx, "gcp-public-data-arco-era5/co/model-level-moisture.zarr-v2")
	assert.Nil(t, err)
	byName := make(map[string]*RemoteFile)
	for i := range files {
		rf := &files[i]
		byName[files[i].Name] = rf
		log.Printf("Name: %s, IsDir: %v", rf.Name, rf.IsDir)
	}

	assert.True(t, byName["cc"].IsDir)
	log.Printf("Name: %s, IsDir: %v", byName[".zattrs"].Name, byName[".zattrs"].IsDir)

	assert.False(t, byName[".zattrs"].IsDir)
}
