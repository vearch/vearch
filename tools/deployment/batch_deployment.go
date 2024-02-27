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
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pkg/sftp"
	"github.com/vearch/vearch/internal/config"
	"golang.org/x/crypto/ssh"
)

type DepConf struct {
	User     string   `json:"user"`
	Password string   `json:"password"`
	Port     string   `json:"port"`
	Dir      string   `json:"dir"`
	Master   []string `json:"master"`
	Ps       []string `json:"ps"`
	Router   []string `json:"router"`
	Copy     []string `json:"copy"`
}

var conf DepConf

var confDir string

func init() {
	flag.StringVar(&confDir, "dir", "", "dir path in dir has some configs")
}

func main() {
	flag.Parse()

	if confDir == "" {
		confDir = "."
	}

	args := flag.Args()

	if len(args) != 2 {
		fmt.Println("you must set ctrl (batch_deployment dir=[your_deploy_config_dir] [bin_path] [deploy stop start destroy])")
		os.Exit(1)
	}

	binPath := args[0]

	bytes, err := os.ReadFile(confDir + "/conf.json")

	if err != nil {
		panic(err)
	}

	conf = DepConf{}

	err = json.Unmarshal(bytes, &conf)

	if err != nil {
		fmt.Println("unmarshal conf by json err:")
		panic(err)
	}

	config := &ssh.ClientConfig{
		User: conf.User,
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}

	if conf.Password != "" {
		config.Auth = []ssh.AuthMethod{
			ssh.Password(conf.Password),
		}
	} else {
		fmt.Println("login begin and password is empty")
	}

	ipMap := make(map[string]bool)

	for _, ip := range conf.Master {
		ipMap[ip] = true
	}

	for _, ip := range conf.Ps {
		ipMap[ip] = true
	}

	for _, ip := range conf.Router {
		ipMap[ip] = true
	}

	if args[1] == "deploy" {
		deploy(ipMap, config, binPath)
	} else if args[1] == "start" {
		start(config, binPath)
	} else if args[1] == "stop" {
		stop(config, binPath)
	} else if args[1] == "destroy" {
		stop(config, binPath)
		destroy(ipMap, config, binPath)
	} else if args[1] == "status" {
		status(config, binPath)
	} else {
		log.Fatal("unknow args ", args)
	}

}

func status(cf *ssh.ClientConfig, s string) {

	var buffer bytes.Buffer

	tag := "master"
	for _, ip := range conf.Master {
		client := createClient(ip, conf, cf)
		command := runCommand(client, "ps -ef|grep `cat "+conf.Dir+"/"+tag+".pid`|grep -v grep|grep "+tag)
		if !strings.Contains(string(command), "config.toml") {
			buffer.Write([]byte(ip + " " + tag + " not run \n"))
		}
		client.Close()
	}

	tag = "router"
	for _, ip := range conf.Router {
		client := createClient(ip, conf, cf)
		command := runCommand(client, "ps -ef|grep `cat "+conf.Dir+"/"+tag+".pid`|grep -v grep|grep "+tag)
		if !strings.Contains(string(command), "config.toml") {
			buffer.Write([]byte(ip + " " + tag + " not run \n"))
		}
		client.Close()
	}

	tag = "ps"
	for _, ip := range conf.Ps {
		client := createClient(ip, conf, cf)
		command := runCommand(client, "ps -ef|grep `cat "+conf.Dir+"/"+tag+".pid`|grep -v grep|grep "+tag)
		if !strings.Contains(string(command), "config.toml") {
			buffer.Write([]byte(ip + " " + tag + " not run \n"))
		}
		client.Close()
	}

	if buffer.Len() == 0 {
		log.Println("all server is running ok !!!!!!")
	} else {
		fmt.Println(buffer.String())
	}

}

func destroy(ipMap map[string]bool, cf *ssh.ClientConfig, binPath string) {

	cbf := new(config.Config)
	_, err := toml.DecodeFile(confDir+"/config.toml", cbf)
	if err != nil {
		log.Fatal(err)
	}

	for ip := range ipMap {
		client := createClient(ip, conf, cf)
		removeDir(client, conf.Dir)

		removeDir(client, cbf.Global.Log)
		if len(cbf.Global.Data) > 0 {
			for _, dp := range cbf.Global.Data {
				removeDir(client, dp)
			}
		}

		client.Close()
	}

	for _, ip := range conf.Master {

		client := createClient(ip, conf, cf)

		removeDir(client, cbf.Global.Log)
		if len(cbf.Global.Data) > 0 {
			removeDir(client, cbf.Global.Data[0])
		}
		client.Close()
	}

	for _, ip := range conf.Router {
		client := createClient(ip, conf, cf)
		removeDir(client, cbf.GetDataDir())
		removeDir(client, cbf.GetLogDir())
		client.Close()
	}

	for _, ip := range conf.Ps {
		client := createClient(ip, conf, cf)
		for _, pd := range cbf.GetDatas() {
			removeDir(client, pd)
		}
		removeDir(client, cbf.GetLogDir())
		client.Close()
	}
}

func stop(config *ssh.ClientConfig, binPath string) {

	for _, ip := range conf.Master {
		client := createClient(ip, conf, config)
		runCommand(client, "cd "+conf.Dir+"; ./stop.sh master \n")
		client.Close()
	}

	for _, ip := range conf.Router {
		client := createClient(ip, conf, config)
		runCommand(client, "cd "+conf.Dir+"; ./stop.sh router \n")
		client.Close()
	}

	for _, ip := range conf.Ps {
		client := createClient(ip, conf, config)
		runCommand(client, "cd "+conf.Dir+"; ./stop.sh ps \n")
		client.Close()
	}
}

func start(config *ssh.ClientConfig, binPath string) {

	for _, ip := range conf.Master {
		client := createClient(ip, conf, config)
		runCommand(client, "cd "+conf.Dir+"; ./start.sh master \n")
		client.Close()
	}

	for _, ip := range conf.Router {
		client := createClient(ip, conf, config)
		runCommand(client, "cd "+conf.Dir+"; ./start.sh router \n")
		client.Close()
	}

	for _, ip := range conf.Ps {
		client := createClient(ip, conf, config)
		runCommand(client, "cd "+conf.Dir+"; ./start.sh ps \n")
		client.Close()
	}
}

func deploy(ipMap map[string]bool, config *ssh.ClientConfig, binPath string) {
	now := time.Now()
	wg := sync.WaitGroup{}
	for ip := range ipMap {
		wg.Add(1)
		go func(ip string) {
			client := createClient(ip, conf, config)
			defer client.Close()
			copyFile(client, confDir+"/config.toml", conf.Dir+"/config.toml")
			copyFile(client, confDir+"/start.sh", conf.Dir+"/start.sh")
			copyFile(client, confDir+"/stop.sh", conf.Dir+"/stop.sh")
			copyFile(client, binPath, conf.Dir+"/vearch")
			for i := range conf.Copy {
				copyDir(client, conf.Copy[i], path.Join(conf.Dir, "copy"))
			}

			runCommand(client, "chmod u+x "+conf.Dir+"/start.sh")
			runCommand(client, "chmod u+x "+conf.Dir+"/stop.sh")
			runCommand(client, "chmod u+x "+conf.Dir+"/vearch")
			wg.Done()
		}(ip)
	}
	wg.Wait()

	log.Println("deploy all ok use time ", time.Now().Sub(now))
}

func runCommand(client *ssh.Client, shell string) []byte {
	session, err := client.NewSession()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		session.Close()
	}()
	buf, _ := session.CombinedOutput(shell)
	log.Println("execute command ", client.Conn.RemoteAddr(), " run:[", shell, "] result:[", string(buf), "]")

	return buf
}

var rm string

func removeDir(client *ssh.Client, remoteDir string) {
	if remoteDir == "" || remoteDir == "/" || strings.Contains(remoteDir, "*") {
		fmt.Println("skip remove dir ", client.Conn.RemoteAddr(), remoteDir)
		return
	}

	fmt.Println("remove dir ", client.Conn.RemoteAddr(), remoteDir)
	command := runCommand(client, "/usr/bin/rm -rf "+remoteDir)

	if strings.Contains(string(command), "/usr/bin/rm") {
		runCommand(client, "/bin/rm -rf "+remoteDir)
	}
}

func copyDir(client *ssh.Client, pathDir, remoteDir string) {
	if exists, err := pathExists(pathDir); err != nil {
		panic(err)
	} else if !exists {
		panic("path not exists: " + pathDir)
	}

	var dirName string

	if info, err := os.Stat(pathDir); err != nil {
		panic(err)
	} else if !info.IsDir() {
		copyFile(client, pathDir, path.Join(remoteDir, info.Name()))
		return
	} else {
		dirName = info.Name()
	}

	err := filepath.Walk(pathDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		if relPath, err := filepath.Rel(pathDir, path); err != nil {
			return err
		} else {
			copyFile(client, path, filepath.Join(remoteDir, dirName, relPath))
		}

		return nil
	})

	if err != nil {
		panic(err)
	}

}

func copyFile(client *ssh.Client, localFilePath, remoteFilePath string) {
	if exists, err := pathExists(localFilePath); err != nil {
		panic(err)
	} else if !exists {
		panic("path not exists: " + localFilePath)
	}
	srcFile, err := os.Open(localFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer srcFile.Close()

	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		log.Fatal(err, client.Conn.RemoteAddr(), localFilePath, remoteFilePath)
	}
	defer sftpClient.Close()

	fmt.Println("copy ", client.Conn.RemoteAddr(), localFilePath, " to remote :", remoteFilePath)

	remoteDir := remoteFilePath[0:strings.LastIndex(remoteFilePath, "/")]

	if err = sftpClient.MkdirAll(remoteDir); err != nil {
		if err = sftpClient.Remove(remoteDir); err == nil {
			err = sftpClient.MkdirAll(remoteDir)
		}
	}

	if err != nil {

		log.Fatal(err, " ", client.Conn.RemoteAddr(), " ", localFilePath, " ", remoteDir)
	}

	dstFile, err := sftpClient.Create(remoteFilePath)
	if err != nil {
		log.Fatal(err, " ", client.Conn.RemoteAddr(), " ", localFilePath, " ", remoteDir)
	}
	defer dstFile.Close()

	buf := make([]byte, 102400)
	for {
		n, _ := srcFile.Read(buf)
		if n == 0 {
			break
		}
		dstFile.Write(buf[0:n])
	}

}

func createClient(ip string, conf DepConf, config *ssh.ClientConfig) *ssh.Client {
	port := "22"
	if conf.Port != "" {
		port = conf.Port
	}
	fmt.Println("connect " + ip + ":" + port)
	client, err := ssh.Dial("tcp", ip+":"+port, config)
	if err != nil {
		panic("Failed to dial: " + err.Error())
	}
	return client
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
