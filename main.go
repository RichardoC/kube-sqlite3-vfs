package main

import (

	// "fmt"
	"database/sql"
	"fmt"
	"log"
	"path/filepath"

	// "path/filepath"

	// "github.com/google/uuid"
	"github.com/RichardoC/kube-sqlite3-vfs/pkg/vfs"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
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

	// // register the custom kube-sqlite3-vfs vfs with sqlite
	// // the name specifed here must match the `vfs` param
	// // passed to sql.Open in the dataSourceName:
	// // e.g. `...?vfs=kube-sqlite3-vfs`
	err = sqlite3vfs.RegisterVFS("kube-sqlite3-vfs", vfsN)
	if err != nil {
		logger.Panicw("Failed to Register VFS", "error", err)
	}

	// // file0 is the name of the file stored in kubernetes
	// // The `vfs=kube-sqlite3-vfs` instructs sqlite to use the custom vfs implementation.
	// // The name must match the name passed to `sqlite3vfs.RegisterVFS`
	db, err := sql.Open("sqlite3", "file2.db?_journal=MEMORY&_cache_size=-256&vfs=kube-sqlite3-vfs")
	if err != nil {
		logger.Panic(err)
	}
	defer db.Close()
	// db.SetMaxOpenConns(1)

	_, err = db.Exec("PRAGMA page_size=65536")
	if err != nil {
		logger.Panic(err)
	}

	a, err := db.Exec(`CREATE TABLE IF NOT EXISTS books (
	id text NOT NULL PRIMARY KEY,
	title text
	)`)
	if err != nil {
		logger.Debug(a)
		logger.Debug(a.LastInsertId())
		logger.Debug(a.RowsAffected())
		logger.Panic(err)
	}
	tx, err := db.Begin()
	if err != nil {
		logger.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO books (id, title) VALUES (?, ?)")
	if err != nil {
		logger.Error(err)
	}
	defer stmt.Close()
	totalToInsert := 1000
	for i := 0; i < totalToInsert; i++ {
		_, err = stmt.Exec(uuid.NewString(), fmt.Sprintf("%d", i))
		if err != nil {
			logger.Error(err)
		}
		if (10 *i)%(totalToInsert) == 0{
			logger.Warnf("inserted %d of %d", i, totalToInsert)
		}
	}

	err = tx.Commit()
	if err != nil {
		logger.Fatal(err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM books")
	if err != nil {
		logger.Panic(err)
	}

	var count int
	rows.Next()
	err = rows.Scan(&count)
	logger.Infof("Got %d rows of books with err %s", count, err)

	// explicitly close out the old query since we only allow one at a time
	rows.Close()

	rows, err = db.Query("SELECT id, title FROM books LIMIT 100")
	if err != nil {
		logger.Panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id   string
			name string
		)
		if err := rows.Scan(&id, &name); err != nil {
			logger.Error(err)
			continue
		}
		logger.Infof("ID: %s, Name: %s", id, name)

	}
}
