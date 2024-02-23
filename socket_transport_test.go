package treeply

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRequest(t *testing.T) {
	_, req := getCommand(&FileClient{}, []byte("{\"Type\": \"listdir\", \"Payload\": {\"Path\": \".\"}}"))
	assert.Equal(t, &ListDirReq{Path: "."}, req)
}
