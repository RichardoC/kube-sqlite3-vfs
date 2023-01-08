package kube

import (
	"context"
	"encoding/base32"
	"errors"

	"github.com/psanford/sqlite3vfs"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

const (
	LockFileName = "lockfile"
	ChunkSize    = 64 * 1024
)

// Only var because this can't be a const
var (
	ChunkLabel = labels.SelectorFromSet(map[string]string{"data": "chunk"}).String()
)

type vfs struct {
	kc      *kubernetes.Clientset
	file    string // actually the namespace?
	table   string
	ownerID string
	logger  *zap.SugaredLogger
}

func (v *vfs) namespaceName() string {
	return string(v.b32ByteFromString(v.file))
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

func (v *vfs) Close() error {
	// TODO, remove locks/etc
	return nil
}

func (v *vfs) Truncate(size int64) error {
	// TODO, remove any cms with a number higher than the next chunk
	// then remove the last bit of the previous chunk
	return nil
}

func (v *vfs) FileSize() (int64, error) {
	// TODO, get the number of matching data configmaps, and return that numer * 64k
	cms, err := v.kc.CoreV1().ConfigMaps(v.namespaceName()).List(context.TODO(), metav1.ListOptions{LabelSelector: ChunkLabel})
	if err != nil {
		return 0, nil
	}
	fSize := int64(len(cms.Items)) * ChunkSize
	return fSize, nil
}

func (v *vfs) ReadAt(p []byte, off int64) (n int, err error) {
	// TODO, work out which chunks we need, and which bytes from those
	// then have other functions to do those directly
	// and then cat together and return

}

func (v *vfs) WriteAt(p []byte, off int64) (n int, err error) {

}


// Sync noops as we're doing the writes directly
func (v *vfs) Sync(flag sqlite3vfs.SyncType) error {
	return nil
}

// TODO actually have a lock configmap with whatever's needed
func (v *vfs) Lock(elock sqlite3vfs.LockType) error {
	return nil
}

func (v *vfs) Unlock(elock sqlite3vfs.LockType) error {
	return nil
}

func (v *vfs) SectorSize() int64 {
	// 64k as we're considering each chunk as a sector
	return ChunkSize
}

// DeviceCharacteristics
// We'll target 64K per configmap
func (v *vfs) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.IocapAtomic64K
}

func (v *vfs) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	// in case we're racing another client
	for i := 0; i < 100; i++ {
		// Check if namespace and lockfile already exist.
		// If they don't, create them
		// if this fails, return readonlyfs
		_, err := v.kc.CoreV1().Namespaces().Get(context.TODO(), v.namespaceName(), metav1.GetOptions{})
		if kerrors.IsNotFound(err) {
			// Create namespace
			ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v.namespaceName()}}
			_, err := v.kc.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{})
			if err != nil {
				v.logger.Error(err)
				continue
			}
		} else if err != nil {
			v.logger.Error(err)
			continue
		}

		// Now check for lock file
		_, err = v.kc.CoreV1().ConfigMaps(v.namespaceName()).Get(context.TODO(), LockFileName, metav1.GetOptions{})
		if kerrors.IsNotFound(err) {
			lf := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: LockFileName}}
			_, err := v.kc.CoreV1().ConfigMaps(v.namespaceName()).Create(context.TODO(), lf, metav1.CreateOptions{})
			if err != nil {
				v.logger.Error(err)
				continue
			}
		}
		return v, flags, nil

	}

	return nil, flags, errors.New("failed to get/create file metadata too many times due to races")

}

func (v *vfs) Delete(name string, dirSync bool) error {
	// in case we're racing another client
	for i := 0; i < 100; i++ {
		err := v.kc.CoreV1().Namespaces().Delete(context.TODO(), v.namespaceName(), metav1.DeleteOptions{})
		v.logger.Error(err)
		return sqlite3vfs.IOError
	}
	v.logger.Errorw("Failed to delete file", "filename", name, "dirSync", dirSync)
	return sqlite3vfs.IOError
}

// Access tests for access permission. Returns true if the requested permission is available.
// TODO, actually fulfil this. Probably check if we can get configmaps in the namespace?
func (v *vfs) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	return true, nil
}

// FullPathname returns the canonicalized version of name.
// TODO actually fulfil this
func (v *vfs) FullPathname(name string) string {
	return name
}

// CheckReservedLock
// TODO actually fulfil this
func (v *vfs) CheckReservedLock() (bool, error) {
	return false, nil
}
