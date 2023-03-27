package main

import (

	// "fmt"
	"bytes"
	"database/sql"
	"log"
	"os"
	"path/filepath"

	// "path/filepath"

	// "github.com/google/uuid"
	"github.com/RichardoC/kube-sqlite3-vfs/pkg/vfs"
	"github.com/psanford/sqlite3vfs"

	_ "github.com/mattn/go-sqlite3"
	"github.com/thought-machine/go-flags"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	// "k8s.io/client-go/tools/clientcmd"
	// "k8s.io/client-go/util/homedir"
)

type Options struct {
	KubeConfig string `long:"kubeconfig" description:"(optional) absolute path to the kubeconfig file"`
	FileName   string `long:"filename" description:"name of the sqlite3 database file to test with" default:"/home/richardf/gitclones/kube-sqlite3-vfs/file2.db"`
	Verbose    bool   `long:"verbosity" short:"v" description:"Uses zap Development default verbose mode rather than production"`
	Retries    int    `long:"retries" description:"Number of retries for API calls" default:"1"`
}

func main() {
	var opts Options
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		log.Panicf("can't parse flags: %v", err)
	}

	var lg *zap.Logger
	var logger *zap.SugaredLogger

	if opts.Verbose {
		lg, err = zap.NewDevelopment()
		if err != nil {
			log.Panicf("can't initialize zap logger: %v", err)
		}
	} else {
		lg, err = zap.NewProduction()
		if err != nil {
			log.Panicf("can't initialize zap logger: %v", err)
		}

	}

	defer lg.Sync()
	logger = lg.Sugar()
	defer logger.Sync()

	// Send standard logging to zap
	undo := zap.RedirectStdLog(lg)
	defer undo()

	logger.Infow("Got config", "opts", opts)

	var kubeconfig string
	if opts.KubeConfig != "" {
		kubeconfig = opts.KubeConfig
	} else if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")

	}

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		logger.Panic(err)
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Panic(err)
	}

	vfsN := vfs.NewVFS(clientset, "test", logger, opts.Retries)

	fn := "file2.db"

	vfsN.Open(fn, sqlite3vfs.OpenFlag(0))

	f, err := os.Open(opts.FileName)
	if err != nil {
		logger.Panic(err)
	}
	defer f.Close()

	// b1 := make([]byte, vfs.SectorSize)

	fileA := vfs.NewFile(fn, vfsN)

	err = nil

	fi, err := f.Stat()
	if err != nil {
		// Could not obtain stat, handle error
		logger.Error(err)
	}

	fSize := fi.Size()

	allBytesReal := make([]byte, fSize)

	n, err := f.Read(allBytesReal)
	if err != nil || n != int(fSize) {
		logger.Errorw("Failed to read all source file", "err", err, "bytesRead", n)
	}

	f.Seek(0, 0)

	n, err = fileA.WriteAt(allBytesReal, 0)

	if err != nil || n != int(fSize) {
		logger.Errorw("Failed to write all source file", "err", err, "bytesWritten", n, "trueLength", fSize)
	}

	// for err != io.EOF {
	// 	var n1 int
	// 	n1, err = f.Read(b1)
	// 	if err == io.EOF && n1 == 0 {
	// 		break
	// 	}
	// 	logger.Infof("Writing sector %d", index)
	// 	if err != nil && err != io.EOF {
	// 		logger.Panic(err)
	// 	}
	// 	sect := &vfs.Sector{Index: index, Data: b1[:n1], Labels: fileA.SectorLabels}

	// 	err = fileA.WriteSector(sect)
	// 	if err != nil {
	// 		logger.Panic(err)
	// 	}
	// 	index += 1

	// }

	// Confirm that remote and local files the same size

	rSize, err := fileA.FileSize()

	if err != nil {
		logger.Error(err)
	}

	if fSize != rSize {
		logger.Errorw("Size mismatch", "localFileSize", fSize, "remotefileSize", rSize)
	} else {
		logger.Info("Local and remote file sizes are the same")
	}

	// Confirm that the remote file is the same as the local one

	allBytes := make([]byte, fSize)

	bytesRead, err := fileA.ReadAt(allBytes, 0)

	if err != nil {
		logger.Error(err)
	}
	if int64(bytesRead) != fSize {
		logger.Errorw("Failed to read all bytes", "readBytes", bytesRead, "trueLengthBytes", fSize)
	}

	// f.Seek(0,0)
	// allBytesReal := make([]byte, fSize)

	// n, err := f.Read(allBytesReal)
	// if err != nil || n != int(fSize){
	// 	logger.Errorw("Failed to read all source file", "err", err, "bytesRead", n)
	// }

	if bytes.Equal(allBytesReal, allBytes) {
		logger.Info("Read the same bytes we wrote")
	} else {
		logger.Error("Failed to read the same bytes we wrote")
	}

	// register the custom kube-sqlite3-vfs vfs with sqlite
	// the name specifed here must match the `vfs` param
	// passed to sql.Open in the dataSourceName:
	// e.g. `...?vfs=kube-sqlite3-vfs`
	err = sqlite3vfs.RegisterVFS("kube-sqlite3-vfs", vfsN)
	if err != nil {
		logger.Panicw("Failed to Register VFS", "error", err)
	}

	db, err := sql.Open("sqlite3", "file2.db?_query_only=true&_journal=OFF&mode=ro&sqlite_os_trace=&vfs=kube-sqlite3-vfs")
	// db, err := sql.Open("sqlite3", "file2.db?_query_only=true&_journal=OFF&mode=ro&vfs=kube-sqlite3-vfs")
	if err != nil {
		logger.Panic(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)

	// _, err = db.Exec("PRAGMA journal_mode = MEMORY;") // So we can ignore file creation for now
	// if err != nil {
	// 	logger.Panic(err)
	// }	
	// _, err = db.Exec("PRAGMA temp_store=MEMORY;") // So we can ignore file creation for now
	// if err != nil {
	// 	logger.Panic(err)
	// }

	rows, err := db.Query("SELECT COUNT(*) FROM books")
	if err != nil {
		logger.Panic(err)
	}
	logger.Infof("%+v", rows)
	var count int
	rows.Next()
	err = rows.Scan(&count)
	logger.Infof("Got %d rows of books with err %s", count, err)

}
