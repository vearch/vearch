// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/go-resty/resty/v2"
)

type ListSpaceResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
	Data []struct {
		ID           int    `json:"id"`
		Name         string `json:"name"`
		ResourceName string `json:"resource_name"`
		Version      int    `json:"version"`
		DbID         int    `json:"db_id"`
		Enabled      bool   `json:"enabled"`
		Partitions   []struct {
			ID                int   `json:"id"`
			SpaceID           int   `json:"space_id"`
			DbID              int   `json:"db_id"`
			PartitionSlot     int   `json:"partition_slot"`
			Replicas          []int `json:"replicas"`
			ResourceExhausted bool  `json:"resourceExhausted"`
		} `json:"partitions"`
		PartitionNum    int             `json:"partition_num"`
		ReplicaNum      int             `json:"replica_num"`
		Properties      json.RawMessage `json:"properties"`
		Engine          json.RawMessage `json:"engine"`
		SpaceProperties json.RawMessage `json:"space_properties"`
	} `json:"data"`
}

type QueryByDocID struct {
	DbName    string `json:"db_name"`
	SpaceName string `json:"space_name"`
	Query     struct {
		DocumentIds []string `json:"document_ids"`
		PartitionID string   `json:"partition_id"`
		Next        bool     `json:"next"`
	} `json:"query"`
	VectorValue bool `json:"vector_value"`
}

type DocInfo struct {
	Code      int    `json:"code"`
	Msg       string `json:"msg"`
	Total     int    `json:"total"`
	Documents []struct {
		ID     string          `json:"_id"`
		Source json.RawMessage `json:"_source"`
	} `json:"documents"`
}

type Document struct {
	DbName    string            `json:"db_name"`
	SpaceName string            `json:"space_name"`
	Documents []json.RawMessage `json:"documents"`
}

func dump(url string, db string, space string, output string) error {
	client := resty.New()

	listSpaceUrl := fmt.Sprintf("%s/list/space?db=%s", url, db)

	spaceSchema := &ListSpaceResponse{}
	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(`{"name":"John Doe","occupation":"gardener"}`).
		SetResult(spaceSchema).
		Get(listSpaceUrl)

	if err != nil {
		fmt.Printf("The HTTP request failed with error %s\n", err)
		return err
	} else {
		fmt.Println(resp)
	}

	partitionIDs := make([]int, 0)
	for _, s := range spaceSchema.Data {
		if s.Name == space {
			fmt.Printf("space %s\n", string(s.SpaceProperties))
			fmt.Printf("engine %s\n", string(s.Engine))
			for _, p := range s.Partitions {
				partitionIDs = append(partitionIDs, p.ID)
			}
		}
	}

	fmt.Printf("partitions %v\n", partitionIDs)

	q := QueryByDocID{
		DbName:    db,
		SpaceName: space,
		Query: struct {
			DocumentIds []string `json:"document_ids"`
			PartitionID string   `json:"partition_id"`
			Next        bool     `json:"next"`
		}{
			DocumentIds: []string{"0"},
			PartitionID: "",
			Next:        true,
		},
		VectorValue: true,
	}

	file, err := os.OpenFile(output+"/data.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}
	defer file.Close()
	doc := &DocInfo{}
	for _, pid := range partitionIDs {
		getDocUrl := fmt.Sprintf("%s/document/query", url)

		for i := -1; ; i++ {
			if i == -1 {
				q.Query.DocumentIds = []string{"0"}
				q.Query.Next = false
			} else {
				q.Query.DocumentIds = []string{fmt.Sprint(i)}
				q.Query.Next = true
			}
			q.Query.PartitionID = fmt.Sprint(pid)
			resp, err := client.R().
				SetHeader("Content-Type", "application/json").
				SetBody(q).
				SetResult(doc).
				Post(getDocUrl)
			if err != nil {
				fmt.Printf("resp %v err %v\n", resp, err)
				break
			}

			if doc.Total == 0 {
				break
			}
			_, err = file.WriteString(string(doc.Documents[0].Source) + "\n")
			if err != nil {
				log.Fatalf("failed to write to file: %s", err)
			}
		}
	}
	return nil
}

func restore(url string, db string, space string, path string) error {
	client := resty.New()

	upsertSpaceUrl := fmt.Sprintf("%s/document/upsert", url)

	doc := &Document{
		DbName:    db,
		SpaceName: space,
		Documents: make([]json.RawMessage, 1),
	}
	file, err := os.OpenFile(path+"/data.txt", os.O_RDONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open file: %s", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		doc.Documents[0] = line
		resp, err := client.R().
			SetHeader("Content-Type", "application/json").
			SetBody(doc).
			Post(upsertSpaceUrl)
		if err != nil {
			fmt.Printf("resp %v err %v\n", resp, err)
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func main() {
	var command, url, db, space, output, input string

	flag.StringVar(&command, "command", "create", "backup command")
	flag.StringVar(&url, "url", "", "vearch url")
	flag.StringVar(&db, "db", "", "vearch db name")
	flag.StringVar(&space, "space", "", "vearch space name")
	flag.StringVar(&output, "output", "data", "create output directory")
	flag.StringVar(&input, "input", "data", "restore input directory")
	flag.Parse()

	switch command {
	case "create":
		err := dump(url, db, space, output)
		if err != nil {
			fmt.Printf("err %v\n", err)
			os.Exit(1)
		}
	case "restore":
		err := restore(url, db, space, input)
		if err != nil {
			fmt.Printf("err %v\n", err)
			os.Exit(1)
		}
	}
}
