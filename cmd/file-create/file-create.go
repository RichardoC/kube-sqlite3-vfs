package main

import (

	// "fmt"
	"bytes"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"path/filepath"

	// "path/filepath"

	// "github.com/google/uuid"
	"github.com/RichardoC/kube-sqlite3-vfs/pkg/vfs"

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
	// FileName   string `long:"filename" description:"name of the sqlite3 database file to test with" default:"/home/richardf/gitclones/kube-sqlite3-vfs/file2.db"`
	Verbose bool `long:"verbosity" short:"v" description:"Uses zap Development default verbose mode rather than production"`
	Retries int  `long:"retries" description:"Number of retries for API calls" default:"1"`
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

	logger.Infoln(os.Getwd())

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

	fn := "fakefile.txt"

	f, err := os.OpenFile("fn", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Fatal(err)
	}

	// vfsN.Open(fn, sqlite3vfs.OpenFlag(0))

	// f, err := os.Open(opts.FileName)
	// if err != nil {
	// 	logger.Panic(err)
	// }
	// defer f.Close()

	// b1 := make([]byte, vfs.SectorSize)

	fileA := vfs.NewFile(fn, vfsN)

	dualWrite := func(p []byte, off int64) error {
		// (int, error)
		n1, err1 := f.WriteAt(p, off)
		if err1 != nil {
			return err1
		}
		n2, err2 := fileA.WriteAt(p, off)
		if err2 != nil {
			return err2
		}
		if n1 != n2 {
			return fmt.Errorf("Wrong no of bytes read %d, %d", n1, n2)
		}
		return nil

	}

	dualRead := func(pFile []byte, pVFS []byte, off int64) error {
		// (int, error)
		n1, err1 := f.ReadAt(pFile, off)
		n2, err2 := fileA.ReadAt(pVFS, off)
		if n1 != n2 {
			return fmt.Errorf("wrong no of bytes read %d, %d", n1, n2)
		}
		if err1 != nil {
			return err1
		}
		if err2 != nil {
			return err2
		}

		return nil

	}

	toWrite := []byte{1, 2, 3, 4}
	err = dualWrite(toWrite, 0)
	if err != nil {
		// Could not obtain stat, handle error
		logger.Error(err)
	}

	err = nil

	fi, err := f.Stat()
	if err != nil {
		// Could not obtain stat, handle error
		logger.Error(err)
	}

	fSize := fi.Size()

	p1 := make([]byte, fSize)
	p2 := make([]byte, fSize)

	err = dualRead(p1, p2, 0)

	if err != nil {
		// Could not obtain stat, handle error
		logger.Error(err)
	}

	if bytes.Equal(p1, p2) {
		logger.Info("Read the same bytes we wrote")
	} else {
		logger.Error("Failed to read the same bytes we wrote")
	}

	toWrite = []byte{1, 2, 3, 4}
	err = dualWrite(toWrite, 3)
	if err != nil {
		// Could not obtain stat, handle error
		logger.Error(err)
	}

	err = nil

	fi, err = f.Stat()
	if err != nil {
		// Could not obtain stat, handle error
		logger.Error(err)
	}

	fSize = fi.Size()

	p1 = make([]byte, fSize)
	p2 = make([]byte, fSize)

	err = dualRead(p1, p2, 0)

	if err != nil {
		// Could not obtain stat, handle error
		logger.Error(err)
	}

	if bytes.Equal(p1, p2) {
		logger.Info("Read the same bytes we wrote")
	} else {
		logger.Error("Failed to./cmd/file-create/fn read the same bytes we wrote")
	}

	randomToWrite := make([]byte, vfs.SectorSize+2)
	rand.Read(randomToWrite)

	testWriting := func(p []byte, off int64) {

		err = dualWrite(p, off)
		if err != nil {
			// Could not obtain stat, handle error
			logger.Error(err)
		}

		err = nil

		fi, err = f.Stat()
		if err != nil {
			// Could not obtain stat, handle error
			logger.Error(err)
		}

		fSize = fi.Size()

		p1 = make([]byte, fSize/2)
		p2 = make([]byte, fSize/2)

		err = dualRead(p1, p2, fSize/2)

		if err != nil {
			// Could not obtain stat, handle error
			logger.Error(err)
		}

		if bytes.Equal(p1, p2) {
			logger.Info("Read the same bytes we wrote")
		} else {
			logger.Error("Failed to read the same bytes we wrote")
		}
		virtualFileSize, _ := fileA.FileSize()
		if fi.Size() != virtualFileSize {
			logger.Errorw("Mismatched file sizes", "realFile", fi.Size(), "virtualFile", virtualFileSize)
		}

	}

	testWriting(randomToWrite, 0)
	randomToWrite = make([]byte, vfs.SectorSize+2)
	rand.Read(randomToWrite)
	fSize = fi.Size()
	testWriting(randomToWrite, fSize - 46)
	randomToWrite = make([]byte, vfs.SectorSize+2)
	testWriting(randomToWrite, fSize - 46)
	randomToWrite = make([]byte, 2*vfs.SectorSize-1)
	testWriting(randomToWrite, fSize - 46 - vfs.SectorSize)

	p1 = make([]byte, fSize)
	p2 = make([]byte, fSize)

	err = dualRead(p1, p2, fSize)

	if err != nil {
		// Could not obtain stat, handle error
		logger.Error(err)
	}

	if bytes.Equal(p1, p2) {
		logger.Info("Read the same bytes we wrote")
	} else {
		logger.Error("Failed to read the same bytes we wrote")
	}

	for i := 0; i < 10; i++ {
		s := fi.Size()
		offset, _ := rand.Int(rand.Reader, new(big.Int).SetInt64(s))
		length, _ := rand.Int(rand.Reader, new(big.Int).SetInt64(s+1))

		randomToWrite = make([]byte, length.Int64())
		rand.Read(randomToWrite)
		testWriting(randomToWrite, offset.Int64())

		length, _ = rand.Int(rand.Reader, new(big.Int).SetInt64(fi.Size()))
		off := fi.Size() - length.Int64()

		p1 = make([]byte, length.Int64())
		p2 = make([]byte, length.Int64())

		err = dualRead(p1, p2, off)
		if err != nil {
			logger.Error(err)
		}
		virtualFileSize, _ := fileA.FileSize()
		if fi.Size() != virtualFileSize {
			logger.Errorw("Mismatched file sizes", "realFile", fi.Size(), "virtualFile", virtualFileSize)
		}

	}

	// allBytesReal := make([]byte, fSize)

	// n, err := f.Read(allBytesReal)
	// if err != nil || n != int(fSize) {
	// 	logger.Errorw("Failed to read all source file", "err", err, "bytesRead", n)
	// }

	// f.Seek(0, 0)

	// n, err = fileA.WriteAt(allBytesReal, 0)

	// if err != nil || n != int(fSize) {
	// 	logger.Errorw("Failed to write all source file", "err", err, "bytesWritten", n, "trueLength", fSize)
	// }

	// // for err != io.EOF {
	// // 	var n1 int
	// // 	n1, err = f.Read(b1)
	// // 	if err == io.EOF && n1 == 0 {
	// // 		break
	// // 	}
	// // 	logger.Infof("Writing sector %d", index)
	// // 	if err != nil && err != io.EOF {
	// // 		logger.Panic(err)
	// // 	}
	// // 	sect := &vfs.Sector{Index: index, Data: b1[:n1], Labels: fileA.SectorLabels}

	// // 	err = fileA.WriteSector(sect)
	// // 	if err != nil {
	// // 		logger.Panic(err)
	// // 	}
	// // 	index += 1

	// // }

	// // Confirm that remote and local files the same size

	// rSize, err := fileA.FileSize()

	// if err != nil {
	// 	logger.Error(err)
	// }

	// if fSize != rSize {
	// 	logger.Errorw("Size mismatch", "localFileSize", fSize, "remotefileSize", rSize)
	// } else {
	// 	logger.Info("Local and remote file sizes are the same")
	// }

	// // Confirm that the remote file is the same as the local one

	// allBytes := make([]byte, fSize)

	// bytesRead, err := fileA.ReadAt(allBytes, 0)

	// if err != nil {
	// 	logger.Error(err)
	// }
	// if int64(bytesRead) != fSize {
	// 	logger.Errorw("Failed to read all bytes", "readBytes", bytesRead, "trueLengthBytes", fSize)
	// }

	// // f.Seek(0,0)
	// // allBytesReal := make([]byte, fSize)

	// // n, err := f.Read(allBytesReal)
	// // if err != nil || n != int(fSize){
	// // 	logger.Errorw("Failed to read all source file", "err", err, "bytesRead", n)
	// // }

	// if bytes.Equal(allBytesReal, allBytes) {
	// 	logger.Info("Read the same bytes we wrote")
	// } else {
	// 	logger.Error("Failed to read the same bytes we wrote")
	// }

	// // register the custom kube-sqlite3-vfs vfs with sqlite
	// // the name specifed here must match the `vfs` param
	// // passed to sql.Open in the dataSourceName:
	// // e.g. `...?vfs=kube-sqlite3-vfs`
	// err = sqlite3vfs.RegisterVFS("kube-sqlite3-vfs", vfsN)
	// if err != nil {
	// 	logger.Panicw("Failed to Register VFS", "error", err)
	// }

}
