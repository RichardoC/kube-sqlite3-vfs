package main

import (

	// "fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	// "path/filepath"

	// "github.com/google/uuid"
	"github.com/RichardoC/kube-sqlite3-vfs/pkg/vfs"
	"github.com/psanford/sqlite3vfs"

	// _ "github.com/mattn/go-sqlite3"
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

	f, err := os.Open(fn)
	if err != nil {
		logger.Panic(err)
	}
	defer f.Close()

	b1 := make([]byte, vfs.SectorSize)

	fileA := vfs.NewFile(fn, vfsN)

	err = nil

	index := int64(0)

	for err != io.EOF {
		var n1 int
		n1, err = f.Read(b1)
		if err == io.EOF && n1 == 0 {
			break
		}
		logger.Infof("Writing sector %d", index)
		if err != nil && err != io.EOF {
			logger.Panic(err)
		}
		sect := &vfs.Sector{Index: index, Data: b1[:n1], Labels: fileA.SectorLabels}

		err = fileA.WriteSector(sect)
		if err != nil {
			logger.Panic(err)
		}
		index += 1

	}

	// // register the custom kube-sqlite3-vfs vfs with sqlite
	// // the name specifed here must match the `vfs` param
	// // passed to sql.Open in the dataSourceName:
	// // e.g. `...?vfs=kube-sqlite3-vfs`
	// err = sqlite3vfs.RegisterVFS("kube-sqlite3-vfs", vfsN)
	// if err != nil {
	// 	logger.Panicw("Failed to Register VFS", "error", err)
	// }

	// // file0 is the name of the file stored in kubernetes
	// // The `vfs=kube-sqlite3-vfs` instructs sqlite to use the custom vfs implementation.
	// // The name must match the name passed to `sqlite3vfs.RegisterVFS`
	// db, err := sql.Open("sqlite3", "file2.db?vfs=kube-sqlite3-vfs")
	// if err != nil {
	// 	logger.Panic(err)
	// }
	// defer db.Close()
	// // PRAGMA page_size = 2048;
	// // pgma := fmt.Sprintf("PRAGMA page_size = %d", vfs.SectorSize)
	// // _, err = db.Exec(pgma)
	// // if err != nil {
	// // 	logger.Panic(err)
	// // }

	// // Seemed to work if only this?
	// a, err := db.Exec(`CREATE TABLE foo (
	// id text NOT NULL PRIMARY KEY,
	// title text
	// )`)
	// if err != nil {
	// 	logger.Debug(a)
	// 	logger.Panic(err)
	// }
	// // _, err = db.Exec(`CREATE TABLE IF NOT EXISTS foo (
	// // 	id text NOT NULL PRIMARY KEY,
	// // 	title text
	// // 	)`)
	// // if err != nil {
	// // 	logger.Panic(err)
	// // }

	// _, err = db.Exec(`INSERT INTO foo (id, title) values (?, ?)`, "developer-arbitration", "washroom-whitecap")
	// if err != nil {
	// 	logger.Panic(err)
	// }

	// var gotID, gotTitle string
	// row := db.QueryRow("SELECT id, title FROM foo where id = ?", "developer-arbitration")
	// err = row.Scan(&gotID, &gotTitle)
	// if err != nil {
	// 	logger.Panic(err)
	// }

	// logger.Infof("got: id=%s title=%s", gotID, gotTitle)

	// db, err := sql.Open("sqlite3", "file2.db")
	// if err != nil {
	// 	logger.Panic(err)
	// }
	// defer db.Close()

	// a, err := db.Exec(`CREATE TABLE IF NOT EXISTS books (
	// id text NOT NULL PRIMARY KEY,
	// title text
	// )`)
	// if err != nil {
	// 	logger.Debug(a)
	// 	logger.Panic(err)
	// }

	// for i := 0; i < 10000; i++ {
	// 	if i%1000 == 0 {
	// 		logger.Infof("Got to inserting %d", i)
	// 	}
	// 	_, err = db.Exec(`INSERT INTO books (id, title) values (?, ?)`, uuid.NewString(), fmt.Sprintf("%d", i))
	// 	if err != nil {
	// 		logger.Panic(err)
	// 	}
	// }

	// rows, err := db.Query("SELECT COUNT(*) FROM books")
	// if err != nil {
	// 	logger.Panic(err)
	// }
	// logger.Infof("%+v", rows)
	// var count int
	// rows.Next()
	// err = rows.Scan(&count)
	// logger.Infof("Got %d rows of books with err %s", count, err)
}
