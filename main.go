package main

import (
	"database/sql"
	// "fmt"
	"log"
	"path/filepath"

	"github.com/RichardoC/kube-sqlite3-vfs/pkg/vfs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
	"github.com/thought-machine/go-flags"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
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

	vfsN := vfs.NewVFS(clientset, "test",  logger, opts.Retries)

	// register the custom kube-sqlite3-vfs vfs with sqlite
	// the name specifed here must match the `vfs` param
	// passed to sql.Open in the dataSourceName:
	// e.g. `...?vfs=kube-sqlite3-vfs`
	err = sqlite3vfs.RegisterVFS("kube-sqlite3-vfs", vfsN)
	if err != nil {
		logger.Panicw("Failed to Register VFS", "error", err)
	}

	// file0 is the name of the file stored in kubernetes
	// The `vfs=kube-sqlite3-vfs` instructs sqlite to use the custom vfs implementation.
	// The name must match the name passed to `sqlite3vfs.RegisterVFS`
	db, err := sql.Open("sqlite3", "file2.db?vfs=kube-sqlite3-vfs")
	if err != nil {
		logger.Panic(err)
	}
	defer db.Close()
	// PRAGMA page_size = 2048;
	// pgma := fmt.Sprintf("PRAGMA page_size = %d", vfs.SectorSize)
	// _, err = db.Exec(pgma)
	// if err != nil {
	// 	logger.Panic(err)
	// }

	// Seemed to work if only this?
	_, err = db.Exec(`CREATE TABLE foo (
	id text NOT NULL PRIMARY KEY,
	title text
	)`)
	if err != nil {
		logger.Panic(err)
	}
	// _, err = db.Exec(`CREATE TABLE IF NOT EXISTS foo (
	// 	id text NOT NULL PRIMARY KEY,
	// 	title text
	// 	)`)
	// if err != nil {
	// 	logger.Panic(err)
	// }

	_, err = db.Exec(`INSERT INTO foo (id, title) values (?, ?)`, "developer-arbitration", "washroom-whitecap")
	if err != nil {
		logger.Panic(err)
	}

	var gotID, gotTitle string
	row := db.QueryRow("SELECT id, title FROM foo where id = ?", "developer-arbitration")
	err = row.Scan(&gotID, &gotTitle)
	if err != nil {
		logger.Panic(err)
	}

	logger.Infof("got: id=%s title=%s", gotID, gotTitle)

}
