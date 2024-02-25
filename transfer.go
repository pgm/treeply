package treeply

import (
	"context"
	"log"

	"cloud.google.com/go/storage"
)

type GCSOperations struct {
	Client *storage.Client
}

func NewGCSOperations(ctx context.Context) *GCSOperations {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return &GCSOperations{Client: client}
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
