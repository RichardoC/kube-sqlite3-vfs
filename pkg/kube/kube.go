package kube

import (
	"context"
	"encoding/base32"
	"errors"

	"github.com/psanford/sqlite3vfs"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type vfs struct {
	db      *kubernetes.Clientset
	file    string // actually the namespace?
	table   string
	ownerID string
	logger  *zap.SugaredLogger
}

func (v *vfs) b32ByteFromString(s string) []byte {
	sb := []byte(s)
	dst := make([]byte, base32.StdEncoding.EncodedLen(len(sb)))
	base32.StdEncoding.Encode(dst, sb)
	return dst
}

func (v *vfs) stringFromB32Byte(b []byte) (string, error) {

	dst := make([]byte, base32.StdEncoding.DecodedLen(len(b)))
	n, err := base32.StdEncoding.Decode(dst, b)
	if err != nil {
		v.logger.Errorw("decode error:", "error", err)
		return "", err
	}
	dst = dst[:n]
	return string(dst), err
}

type kubeFile struct {
}

// ̣Can probably just be a wrapper over the actual VFS 'file' initialisation
// func (v *vfs) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
// 	// in case we're racing another client
// 	for i := 0; i < 100; i++ {
// 		existing, err := v.db.CoreV1().ConfigMaps(v.file).Get(context.TODO(), string(v.b32ByteFromString(name)), metav1.GetOptions{})
// 		if err != nil {
// 			return nil, 0, err
// 		}
// 		if existing != nil {

// 		}
// 	}

// }

func (v *vfs) DeleteDelete(name string, dirSync bool) error {
	// in case we're racing another client
	for i := 0; i < 100; i++ {
		err := v.db.CoreV1().ConfigMaps(v.file).Delete(context.TODO(), string(v.b32ByteFromString(name)), metav1.DeleteOptions{})
		return err
	}
	return errors.New("failed to delete file due to excessive tries")
}

// Access tests for access permission. Returns true if the requested permission is available.
// TODO, actually fulfil this
func (v *vfs) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	return true, nil
}

// FullPathname returns the canonicalized version of name.
// TODO actually fulfil tis
func (v *vfs) FullPathname(name string) string {
	return name
}
