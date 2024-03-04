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

	"github.com/bytedance/sonic"
	"github.com/go-resty/resty/v2"
	"github.com/vearch/vearch/tools/backup/entity"
)

func dumpSchema(space string, describeSpaceResponse *entity.DescribeSpaceResponse, path string) error {
	schema := &entity.SpaceSchema{
		Name:         space,
		PartitionNum: describeSpaceResponse.Data.PartitionNum,
		ReplicaNum:   describeSpaceResponse.Data.ReplicaNum,
		Engine:       describeSpaceResponse.Data.Schema.Engine,
		Properties:   describeSpaceResponse.Data.Schema.Properties,
	}

	s, _ := sonic.Marshal(schema)
	log.Printf("schema :%s\n", string(s))

	file, err := os.OpenFile(path+"/schema.json", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(string(s))
	return err
}

func dump(url string, db_name string, space_name string, output string) error {
	client := resty.New()

	listSpaceUrl := fmt.Sprintf("%s/space/describe", url)

	describeSpaceResponse := &entity.DescribeSpaceResponse{}
	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetQueryParams(map[string]string{
			"db_name":    db_name,
			"space_name": space_name,
		}).
		SetResult(describeSpaceResponse).
		Get(listSpaceUrl)

	if err != nil {
		log.Printf("The HTTP request failed with error %s\n", err)
		return err
	}
	log.Println(resp)

	err = dumpSchema(space_name, describeSpaceResponse, output)
	if err != nil {
		log.Fatalf("failed to dump schema: %s", err)
		return err
	}

	partitionIDs := make([]int, 0)
	for _, p := range describeSpaceResponse.Data.Partitions {
		partitionIDs = append(partitionIDs, p.Pid)
	}

	log.Printf("partitions %v\n", partitionIDs)

	q := entity.QueryByDocID{
		DbName:    db_name,
		SpaceName: space_name,
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
	doc := &entity.DocInfo{}
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
				log.Printf("resp %v err %v\n", resp, err)
				break
			}

			if doc.Total == 0 {
				break
			}
			_, err = file.WriteString(string(doc.Documents[0].Source) + "\n")
			if err != nil {
				log.Fatalf("failed to write to file: %s", err)
				return err
			}
		}
	}
	return nil
}

func restoreSpace(url string, db_name string, path string) error {
	client := resty.New()
	restoreDbUrl := fmt.Sprintf("%s/db/_create", url)
	restoreSpaceUrl := fmt.Sprintf("%s/space/%s/_create", url, db_name)

	createDbBody := &entity.CreateDbBody{
		Name: db_name,
	}
	createDbResponse := &entity.CreateDbResponse{}
	resp, err := client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(createDbBody).
		SetResult(createDbResponse).
		Put(restoreDbUrl)

	if err != nil {
		log.Printf("create db error %s\n", err)
		return err
	}
	log.Println(resp)

	createSpaceResponse := &entity.CreateSpaceResponse{}

	schemaBytes, err := os.ReadFile(path + "/schema.json")
	if err != nil {
		log.Fatal(err)
		return err
	}


	createSpaceBody := &entity.SpaceSchema{}
	err = sonic.Unmarshal(schemaBytes, createSpaceBody)
	if err != nil {
		log.Printf("create space %s\n", err)
		return err
	}

	resp, err = client.R().
		SetHeader("Content-Type", "application/json").
		SetBody(createSpaceBody).
		SetResult(createSpaceResponse).
		Put(restoreSpaceUrl)

	if err != nil {
		return err
	}
	log.Println(resp)

	return nil
}

func restore(url string, db_name string, space_name string, path string) error {
	err := restoreSpace(url, db_name, path)
	if err != nil {
		log.Printf("restore space error %s\n", err)
		return err
	}

	client := resty.New()

	upsertSpaceUrl := fmt.Sprintf("%s/document/upsert", url)

	doc := &entity.Document{
		DbName:    db_name,
		SpaceName: space_name,
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
			log.Printf("resp %v err %v\n", resp, err)
			break
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
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
			log.Printf("err %v\n", err)
			os.Exit(1)
		}
	case "restore":
		err := restore(url, db, space, input)
		if err != nil {
			log.Printf("err %v\n", err)
			os.Exit(1)
		}
	}
}
