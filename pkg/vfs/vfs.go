package vfs

import (
	"context"
	"encoding/base32"
	"errors"
	"io"

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
	SectorSize   = 64 * 1024
)

// Only var because this can't be a const
var (
	SectorLabel    = map[string]string{"data": "sector"}
	LockfileLabel  = map[string]string{"data": "lockfile"}
	NamespaceLabel = map[string]string{"kube-sqlite3-vfs": "used"}
)

type vfs struct {
	kc *kubernetes.Clientset
	// file   string // actually the namespace?
	logger  *zap.SugaredLogger
	retries int
}

func NewVFS(kc *kubernetes.Clientset, logger *zap.SugaredLogger, retries int) *vfs {
	return &vfs{kc: kc, logger: logger, retries: retries}
}

func (f *file) namespaceName() string {
	return string(f.b32ByteFromString(f.rawName))
}

func (f *file) b32ByteFromString(s string) []byte {
	sb := []byte(s)
	dst := make([]byte, f.encoding.EncodedLen(len(sb)))
	f.encoding.Encode(dst, sb)
	return dst
}

func (f *file) stringFromB32Byte(b []byte) (string, error) {

	dst := make([]byte, f.encoding.DecodedLen(len(b)))
	n, err := f.encoding.Decode(dst, b)
	if err != nil {
		f.vfs.logger.Errorw("decode error:", "error", err)
		return "", err
	}
	dst = dst[:n]
	return string(dst), err
}

func (f *file) Close() error {
	// TODO, remove locks/etc
	return nil
}

func (f *file) Truncate(size int64) error {
	// TODO, remove any cms with a number higher than the next sector
	// then remove the last bit of the previous sector
	return nil
}

func (f *file) FileSize() (int64, error) {
	// TODO, get the number of matching data configmaps, and return that numer * 64k
	cms, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(SectorLabel).String()})
	if err != nil {
		return 0, nil
	}
	fSize := int64(len(cms.Items)) * SectorSize
	return fSize, nil
}

type file struct {
	// dataRowKey string
	rawName string
	// randID     string
	// closed     bool
	vfs      *vfs
	encoding *base32.Encoding

	// lockManager lockManager
}

func (f *file) sectorForPos(pos int64) int64 {
	return pos - (pos % SectorSize)
}

func (f *file) ReadAt(p []byte, off int64) (int, error) {
	// if f.closed {
	// 	return 0, os.ErrClosed
	// }

	firstSector := f.sectorForPos(off)

	fileSize, err := f.FileSize()
	if err != nil {
		return 0, err
	}

	lastByte := off + int64(len(p)) - 1

	lastSector := f.sectorForPos(lastByte)

	var (
		n     int
		first = true
	)
	sectors, err := f.getSectorRange(firstSector, lastSector)
	if err != nil {
		return 0, sqlite3vfs.IOErrorRead
	}
	for _, sect := range sectors {
		if first {
			startIndex := off % SectorSize
			n = copy(p, sect.data[startIndex:])
			first = false
			continue
		}

		nn := copy(p[n:], sect.data)
		n += nn
	}
	if lastByte >= fileSize {
		return n, io.EOF
	}

	return n, nil
}

func (f *file) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, sqlite3vfs.BusyError
}

// Sync noops as we're doing the writes directly
func (f *file) Sync(flag sqlite3vfs.SyncType) error {
	return nil
}

// TODO actually have a lock configmap with whatever's needed
func (f *file) Lock(elock sqlite3vfs.LockType) error {
	return nil
}

func (f *file) Unlock(elock sqlite3vfs.LockType) error {
	return nil
}

func (f *file) SectorSize() int64 {
	// 64k as we're considering each sector
	return SectorSize
}

// DeviceCharacteristics
// We'll target 64K per configmap
func (f *file) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.IocapAtomic64K
}

func newFile(name string, v *vfs) *file {
	o := base32.NewEncoding("abcdefghijklmnopqrstuv0123456789")
	e := o.WithPadding('x')
	return &file{rawName: name, vfs: v, encoding: e}
}

func (v *vfs) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	// in case we're racing another client
	for i := 0; i < v.retries; i++ {
		// Check if namespace and lockfile already exist.
		// If they don't, create them
		// if this fails, return readonlyfs

		f := newFile(name, v)
		_, err := v.kc.CoreV1().Namespaces().Get(context.TODO(), f.namespaceName(), metav1.GetOptions{})
		if kerrors.IsNotFound(err) {
			// Create namespace
			ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.namespaceName(), Labels: NamespaceLabel}}
			_, err := v.kc.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{})
			if err != nil {
				v.logger.Error(err)
				continue
			}
		} else if err != nil {
			f.vfs.logger.Error(err)
			continue
		}

		// Now check for lock file
		_, err = f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).Get(context.TODO(), LockFileName, metav1.GetOptions{})
		if kerrors.IsNotFound(err) {
			lf := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: LockFileName, Labels: SectorLabel}}
			_, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).Create(context.TODO(), lf, metav1.CreateOptions{})
			if err != nil {
				f.vfs.logger.Error(err)
				continue
			}
		}
		return f, flags, nil

	}

	return nil, flags, errors.New("failed to get/create file metadata too many times due to races")

}

func (v *vfs) Delete(name string, dirSync bool) error {
	// in case we're racing another client
	f := newFile(name, v)
	for i := 0; i < 100; i++ {
		err := f.vfs.kc.CoreV1().Namespaces().Delete(context.TODO(), f.namespaceName(), metav1.DeleteOptions{})
		f.vfs.logger.Error(err)
		return sqlite3vfs.IOError
	}
	f.vfs.logger.Errorw("Failed to delete file", "filename", name, "dirSync", dirSync)
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
func (f *file) CheckReservedLock() (bool, error) {
	return false, nil
}
