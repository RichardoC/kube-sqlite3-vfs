package main

import (
	"database/sql"
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
}

func main() {
	var opts Options
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		log.Fatalf("can't parse flags: %v", err)
	}

	var lg *zap.Logger
	var logger *zap.SugaredLogger

	if opts.Verbose {
		lg, err = zap.NewDevelopment()
		if err != nil {
			log.Fatalf("can't initialize zap logger: %v", err)
		}
	} else {
		lg, err = zap.NewProduction()
		if err != nil {
			log.Fatalf("can't initialize zap logger: %v", err)
		}

	}

	defer lg.Sync()
	logger = lg.Sugar()
	defer logger.Sync()

	// Send standard logging to zap
	undo := zap.RedirectStdLog(lg)
	defer undo()

	var kubeconfig string
	if opts.KubeConfig != "" {
		kubeconfig = opts.KubeConfig
	} else if home := homedir.HomeDir(); home != "" {
		kubeconfig = filepath.Join(home, ".kube", "config")

	}

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	vfs := vfs.NewVFS(clientset, logger)

	// register the custom donutdb vfs with sqlite
	// the name specifed here must match the `vfs` param
	// passed to sql.Open in the dataSourceName:
	// e.g. `...?vfs=donutdb`
	err = sqlite3vfs.RegisterVFS("kube-sqlite3-vfs", vfs)
	if err != nil {
		logger.Fatalw("Failed to Register VFS", "error", err)
	}

	// file0 is the name of the file stored in dynamodb
	// you can have multiple db files stored in a single dynamodb table
	// The `vfs=donutdb` instructs sqlite to use the custom vfs implementation.
	// The name must match the name passed to `sqlite3vfs.RegisterVFS`
	db, err := sql.Open("sqlite3", "file0.db?vfs=kube-sqlite3-vfs")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS foo (
id text NOT NULL PRIMARY KEY,
title text
)`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`INSERT INTO foo (id, title) values (?, ?)`, "developer-arbitration", "washroom-whitecap")
	if err != nil {
		panic(err)
	}

	var gotID, gotTitle string
	row := db.QueryRow("SELECT id, title FROM foo where id = ?", "developer-arbitration")
	err = row.Scan(&gotID, &gotTitle)
	if err != nil {
		panic(err)
	}

	logger.Infof("got: id=%s title=%s", gotID, gotTitle)
}
