package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/jlaffaye/ftp"
)

// Config структура для данных из config.json
type Config struct {
	MySQLHost       string   `json:"mysql_host"`
	MySQLUser       string   `json:"mysql_user"`
	MySQLPassword   string   `json:"mysql_password"`
	Databases       []string `json:"databases"`
	BackupDirectory string   `json:"backup_directory"`
	FTPHost         string   `json:"ftp_host"`
	FTPUser         string   `json:"ftp_user"`
	FTPPassword     string   `json:"ftp_password"`
	FTPDirectory    string   `json:"ftp_directory"`
}

// Загрузка конфигурации из файла
func loadConfig(filename string) (Config, error) {
	var config Config
	file, err := os.Open(filename)
	if err != nil {
		return config, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	return config, err
}

// Создание резервной копии базы данных
func backupDatabase(config Config, database string, outputFile string) error {
	cmd := exec.Command(
		"mysqldump",
		"-h", config.MySQLHost,
		"-u", config.MySQLUser,
		"-p"+config.MySQLPassword,
		database,
	)
	outfile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create database copy file: %w", err)
	}
	defer outfile.Close()

	cmd.Stdout = outfile
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to execute mysqldump: %w", err)
	}
	return nil
}

// Архивирование файлов в .tar.gz
func archiveFiles(files []string, archivePath string) error {
	tarFile, err := os.Create(archivePath)
	if err != nil {
		return fmt.Errorf("failed to create archive: %w", err)
	}
	defer tarFile.Close()

	gzWriter := gzip.NewWriter(tarFile)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			return fmt.Errorf("failed to get file information %s: %w", file, err)
		}

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return fmt.Errorf("failed to create file header %s: %w", file, err)
		}

		header.Name = filepath.Base(file)
		err = tarWriter.WriteHeader(header)
		if err != nil {
			return fmt.Errorf("failed to write file header into archive: %w", err)
		}

		fileContent, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", file, err)
		}
		defer fileContent.Close()

		_, err = io.Copy(tarWriter, fileContent)
		if err != nil {
			return fmt.Errorf("failed to write file %s into archive: %w", file, err)
		}
	}

	return nil
}

// Загрузка архива на FTP
func uploadToFTP(config Config, localFile string) error {
	conn, err := ftp.Dial(config.FTPHost)
	if err != nil {
		return fmt.Errorf("failed to connect to ftp server: %w", err)
	}
	defer conn.Quit()

	err = conn.Login(config.FTPUser, config.FTPPassword)
	if err != nil {
		return fmt.Errorf("failed to auth on ftp server: %w", err)
	}

	file, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer file.Close()

	remotePath := filepath.Join(config.FTPDirectory, filepath.Base(localFile))
	err = conn.Stor(remotePath, file)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}

	return nil
}

// Основная функция
func main() {
	// Загружаем конфигурацию
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Создаем директорию для резервных копий
	err = os.MkdirAll(config.BackupDirectory, os.ModePerm)
	if err != nil {
		log.Fatalf("failed to create directory for backups: %v", err)
	}

	var backupFiles []string
	for _, db := range config.Databases {
		backupFile := filepath.Join(config.BackupDirectory, db+".sql")
		fmt.Printf("creating database backup %s -> %s\n", db, backupFile)
		err = backupDatabase(config, db, backupFile)
		if err != nil {
			log.Printf("failed to backup database %s: %v", db, err)
			continue
		}
		backupFiles = append(backupFiles, backupFile)
	}

	// Архивируем файлы
	archivePath := filepath.Join(config.BackupDirectory, fmt.Sprintf("backup_%s.tar.gz", time.Now().Format("20060102_150405")))
	fmt.Printf("creating archive -> %s\n", archivePath)
	err = archiveFiles(backupFiles, archivePath)
	if err != nil {
		log.Fatalf("failed to archive: %v", err)
	}

	// Загружаем архив на FTP
	fmt.Printf("uploading -> %s\n", archivePath)
	err = uploadToFTP(config, archivePath)
	if err != nil {
		log.Fatalf("failed to upload: %v", err)
	}

	fmt.Println("Backup completed")
}
