package fileutil

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

// get all fieInfo
func GetAllFiles(dirPth string) (files []os.FileInfo, err error) {
	fis, err := ioutil.ReadDir(filepath.Clean(filepath.ToSlash(dirPth)))
	if err != nil {
		return nil, err
	}

	for _, f := range fis {
		_path := filepath.Join(dirPth, f.Name())

		if f.IsDir() {
			fs, _ := GetAllFiles(_path)
			files = append(files, fs...)
			continue
		}
		files = append(files, f)
	}

	return files, nil
}

// get all fileNames
func GetAllFileNames(dirPth string) (files []string, err error) {
	fis, err := ioutil.ReadDir(filepath.Clean(filepath.ToSlash(dirPth)))
	if err != nil {
		return nil, err
	}

	for _, f := range fis {
		_path := filepath.Join(dirPth, f.Name())

		if f.IsDir() {
			fs, _ := GetAllFileNames(_path)
			files = append(files, fs...)
			continue
		}
		files = append(files, _path)
	}

	return files, nil
}
