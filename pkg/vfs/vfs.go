package vfs

import (
	"context"
	"encoding/base32"
	"errors"
	"fmt"
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
	LockFileNameSuffix = "lockfile"
	SectorSize         = 64 * 1024 // Just until I work out how to make bigger slices for the data
)

// Only var because this can't be a const
var (
	CommonSectorLabel = map[string]string{"data": "sector"}
	LockfileLabel     = map[string]string{"data": "lockfile"}
	// NamespaceLabel = map[string]string{"kube-sqlite3-vfs": "used"}
)

type vfs struct {
	kc *kubernetes.Clientset
	// file   string // actually the namespace?
	logger    *zap.SugaredLogger
	retries   int
	namespace string
}

func NewVFS(kc *kubernetes.Clientset, namespace string, logger *zap.SugaredLogger, retries int) *vfs {
	return &vfs{kc: kc, logger: logger, retries: retries, namespace: namespace}
}

// func (f *file) namespaceName() string {
// 	return string(f.b32ByteFromString(f.RawName))
// }

func (f *file) b32ByteFromString(s string) []byte {
	sb := []byte(s)
	dst := make([]byte, f.encoding.EncodedLen(len(sb)))
	f.encoding.Encode(dst, sb)
	return dst
}

// func (f *file) b32ByteFromBytes(s []byte) []byte {
// 	dst := make([]byte, f.encoding.EncodedLen(len(s)))
// 	f.encoding.Encode(dst, s)
// 	return dst
// }

// func (f *file) bytesFromB32Byte(b []byte) ([]byte, error) {

// 	dst := make([]byte, f.encoding.DecodedLen(len(b)))
// 	n, err := f.encoding.Decode(dst, b)
// 	if err != nil {
// 		f.vfs.logger.Errorw("decode error:", "error", err)
// 		return []byte{}, err
// 	}
// 	dst = dst[:n]
// 	return dst, err
// }

func (f *file) Close() error {

	err := f.setLock(sqlite3vfs.LockNone)

	return err
}

func (f *file) Truncate(size int64) error {

	fileSize, err := f.FileSize()
	if err != nil {
		return err
	}

	if size >= fileSize {
		return nil
	}

	lastRemainingSector := f.sectorForPos(size)

	sect, err := f.getSector(lastRemainingSector)
	if err != nil {
		return err
	}

	sect.Data = sect.Data[:size%SectorSize]

	f.writeSector(sect)

	lastSector := f.sectorForPos(fileSize)

	for sectToDelete := lastRemainingSector; sectToDelete <= lastSector; sectToDelete += 1 {
		err := f.deleteSector(sectToDelete)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *file) FileSize() (int64, error) {
	f.vfs.logger.Debugw("FileSize", "f", f)
	lastcm, err := f.getLastSector()
	if err != nil {
		f.vfs.logger.Error(err)

		return 0, err
	}
	// Could have an off by one error
	size := lastcm.Index*f.SectorSize() + int64(len(lastcm.Data))
	f.vfs.logger.Debugw("FileSize", "f", f, "size", size)

	return size, nil

}

type file struct {
	// dataRowKey string
	RawName string
	// randID     string
	// closed     bool
	vfs          *vfs
	encoding     *base32.Encoding
	SectorLabels map[string]string

	// lockManager lockManager
}

// this needs to return Eof if a read is attempted off the end of the file...
func (f *file) ReadAt(p []byte, off int64) (int, error) {
	f.vfs.logger.Debugw("ReadAt", "off", off, "len(buffer)", len(p))
	// if f.closed {
	// 	return 0, os.ErrClosed
	// }

	firstSector := f.sectorForPos(off)

	fileSize, err := f.FileSize()
	if err != nil {
		f.vfs.logger.Debugw("ReadAt", "off", off, "len(buffer)", len(p), "fileSize", fileSize, "err", err)

		return 0, err
	}

	// if fileSize == 0 {
	// 	f.vfs.logger.Debug("ReadAt found no data to return")
	// 	return 0, nil
	// }

	lastByte := off + int64(len(p)) - 1

	lastSector := f.sectorForPos(lastByte)
	if lastByte > fileSize {
		lastSector = f.sectorForPos(fileSize)
		lastByte = fileSize
	}
	if off >= fileSize {
		f.vfs.logger.Debugw("ReadAt", "off", off, "len(buffer)", len(p), "fileSize", fileSize, "err", io.EOF)
		return 0, io.EOF
	}

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
			n = copy(p, sect.Data[startIndex:])
			first = false
			continue
		}

		nn := copy(p[n:], sect.Data)
		n += nn
	}
	if lastByte >= fileSize {
		return n, io.EOF
	}
	f.vfs.logger.Debugw("ReadAt", "off", off, "len(buffer)", len(p), "n", n)

	if n < len(p) {
		return n, sqlite3vfs.IOErrorShortRead
	}

	return n, nil
}

func (f *file) WriteAt(p []byte, off int64) (n int, err error) {
	f.vfs.logger.Debugw("WriteAt", "len(p)", len(p), "off", off)
	// startSector := f.sectorForPos(off)
	// endSector := f.sectorForPos(off)

	// bytesWritten = 0

	// // if startSector == endSector get it and do the write to the end
	// if startSector == endSector {
	// 	cm, err := f.getSector(startSector)
	// 	if err != nil {
	// 		f.vfs.logger.Error(err)
	// 		return bytesWritten, err
	// 	}
	// 	currentData =
	// }

	// else write the end sector

	//between those sectors do a total fill

	firstSector := f.sectorForPos(off)

	if err != nil {
		return 0, err
	}

	lastByte := off + int64(len(p)) - 1

	lastSector := f.sectorForPos(lastByte)

	var (
		nW    int
		first = true
	)
	sectors, err := f.getSectorRange(firstSector, lastSector)
	if err != nil {
		return 0, sqlite3vfs.IOErrorRead
	}
	for _, sect := range sectors {
		if first {
			startIndex := off % SectorSize
			var bytesToCopy int64
			if len(p) < SectorSize {
				bytesToCopy = int64(len(p))
			} else {
				bytesToCopy = SectorSize - startIndex // bug here, what if our sector is longer than the write?
			}
			sectorData := make([]byte, SectorSize)
			_ = copy(sectorData, sect.Data) // Possible bug here, add logging

			nn := copy(sectorData[startIndex:], p[:bytesToCopy])
			sect.Data = sectorData
			err := f.writeSector(sect)
			if err != nil {
				f.vfs.logger.Error(err)
				return 0, err
			}
			nW += nn
			first = false
			continue
		}

		nn := copy(sect.Data, p[n:])
		err := f.writeSector(sect)
		if err != nil {
			f.vfs.logger.Error(err)
			return n, err
		}
		nW += nn
	}

	return n, nil
}

// Sync noops as we're doing the writes directly
func (f *file) Sync(flag sqlite3vfs.SyncType) error {
	f.vfs.logger.Debugw("Sync", "flag", flag)

	return nil
}

func (f *file) getCurrentLock() (sqlite3vfs.LockType, error) {
	f.vfs.logger.Debugw("getCurrentLock")

	cm, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Get(context.TODO(), f.LockFileName(), metav1.GetOptions{})
	if err != nil {
		f.vfs.logger.Error(err)
		return sqlite3vfs.LockNone, err
	}
	currentLockString := cm.Data["lock"]
	lockToReturn := sqlite3vfs.LockNone
	switch currentLockString {
	case sqlite3vfs.LockNone.String():
		lockToReturn = sqlite3vfs.LockNone
	case sqlite3vfs.LockShared.String():
		lockToReturn = sqlite3vfs.LockShared
	case sqlite3vfs.LockPending.String():
		lockToReturn = sqlite3vfs.LockPending
	case sqlite3vfs.LockExclusive.String():
		lockToReturn = sqlite3vfs.LockExclusive
	case sqlite3vfs.LockReserved.String():
		lockToReturn = sqlite3vfs.LockReserved
	default:
		errStr := fmt.Sprintf("lock type unknown: %v, %v", f, currentLockString)
		f.vfs.logger.Error(errStr)
		return sqlite3vfs.LockNone, errors.New(errStr)
	}
	f.vfs.logger.Debugw("getCurrentLock", "found current lock", lockToReturn)

	return lockToReturn, nil
}

func (f *file) LockFileName() string {
	localLockFileName := fmt.Sprintf("%s-%s", f.b32ByteFromString(f.RawName), LockFileNameSuffix)
	return localLockFileName
}

func (f *file) generateSectorsLabels() {
	fileNameLabel := string(f.b32ByteFromString(f.RawName))

	f.SectorLabels = make(map[string]string)
    for k, v := range CommonSectorLabel {
        f.SectorLabels[k] = v
    }

	f.SectorLabels["relevant-file"] = fileNameLabel
}

// possibly add the validation?
func (f *file) setLock(lock sqlite3vfs.LockType) error {
	f.vfs.logger.Debugw("setLock", "lock", lock)

	lf := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: f.LockFileName(), Labels: LockfileLabel}, Data: map[string]string{"lock": lock.String()}}
	// cm, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Get(context.TODO(), localLockFileName, metav1.GetOptions{})

	_, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Update(context.TODO(), lf, metav1.UpdateOptions{})
	f.vfs.logger.Debugw("setLock", "lock", lock, "err", err)
	if kerrors.IsNotFound(err) {
		lf := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: f.LockFileName(), Labels: LockfileLabel}, Data: map[string]string{"lock": lock.String()}}
		_, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Create(context.TODO(), lf, metav1.CreateOptions{})
		f.vfs.logger.Debugw("setLock has been created", "lock", lock, "err", err)
		return err
	}

	return err

}

// TODO actually have a lock configmap with whatever's needed
func (f *file) Lock(elock sqlite3vfs.LockType) error {
	currentLock, err := f.getCurrentLock()
	if err != nil {
		f.vfs.logger.Error(err)
		return err
	}

	//    UNLOCKED -> SHARED
	//    SHARED -> RESERVED
	//    SHARED -> (PENDING) -> EXCLUSIVE
	//    RESERVED -> (PENDING) -> EXCLUSIVE
	//    PENDING -> EXCLUSIVE

	if elock <= currentLock {
		return nil
	}

	//  (1) We never move from unlocked to anything higher than shared lock.
	if currentLock == sqlite3vfs.LockNone && elock > sqlite3vfs.LockShared {
		return errors.New("invalid lock transition requested")
	}
	//  (2) SQLite never explicitly requests a pendig lock.
	if elock == sqlite3vfs.LockPending {
		return errors.New("invalid Lock() request for state pending")
	}
	//  (3) A shared lock is always held when a reserve lock is requested.
	if elock == sqlite3vfs.LockReserved && currentLock != sqlite3vfs.LockShared {
		return errors.New("can only transition to Reserved lock from Shared lock")
	}

	return f.setLock(elock)

}

func (f *file) Unlock(elock sqlite3vfs.LockType) error {

	currentLock, err := f.getCurrentLock()
	if err != nil {
		f.vfs.logger.Error(err)
		return err
	}

	if elock > sqlite3vfs.LockShared {
		f.vfs.logger.Panicf("Invalid unlock request to level %s", elock)
	}

	if currentLock < elock {
		f.vfs.logger.Panic("Cannot unlock to a level > current lock level")
	}

	if elock == currentLock {
		return nil
	}

	if elock == sqlite3vfs.LockShared {
		return f.setLock(sqlite3vfs.LockShared)
	}

	return f.setLock(sqlite3vfs.LockNone)
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
	f := &file{RawName: name, vfs: v, encoding: e}
	f.generateSectorsLabels()
	return f
}

// TODO, locking so other connections refused?
func (v *vfs) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	v.logger.Debugw("Open", "name", name, "flags", flags)
	// in case we're racing another client

	_, err := v.kc.ServerVersion()
	if err != nil {
		v.logger.Error(err)
		return nil, flags, sqlite3vfs.IOError
	}

	for i := 0; i < v.retries; i++ {
		// Check if namespace and lockfile already exist.
		// If they don't, create them
		// if this fails, return readonlyfs

		f := newFile(name, v)
		// _, err := v.kc.CoreV1().Namespaces().Get(context.TODO(), f.vfs.namespace, metav1.GetOptions{})
		// if kerrors.IsNotFound(err) {
		// 	// Create namespace
		// 	ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: f.vfs.namespace, Labels: NamespaceLabel}}
		// 	_, err := v.kc.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{})
		// 	if err != nil {
		// 		v.logger.Error(err)
		// 		continue
		// 	}
		// } else if err != nil {
		// 	f.vfs.logger.Error(err)
		// 	continue
		// }

		// Now check for lock file
		_, err = f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Get(context.TODO(), f.LockFileName(), metav1.GetOptions{})
		if kerrors.IsNotFound(err) {
			err = f.setLock(sqlite3vfs.LockNone)
			// lf := &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: LockFileName, Labels: LockfileLabel}}
			// _, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Create(context.TODO(), lf, metav1.CreateOptions{})
			if err != nil {
				f.vfs.logger.Error(err)
				continue
			}
		} else if err != nil {
			return f, flags, err
		}

		cms, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(f.SectorLabels).String()})
		v.logger.Debugw("Checked for existing sectors", "sectors", cms, "err", err)
		if err != nil {
			v.logger.Debugw("err response for data configmaps", "error", err)
		}
		if len(cms.Items) == 0 {
			// emptydata := [SectorSize]byte{}
			// err := f.writeSector(&sector{offset: 0, data: emptydata[:]})
			err := f.writeSector(&sector{Index: 0})
			v.logger.Debugw("wrote an empty sector", "error", err)

			if err != nil {
				v.logger.Error(err)
				return f, flags, err

			}

		}
		v.logger.Debugw("Opened file successfully", "name", name, "flags", flags)

		return f, flags, nil

	}
	v.logger.Debugw("Failed to open file")
	return nil, flags, errors.New("failed to get/create file metadata too many times due to races")

}

func (v *vfs) Delete(name string, dirSync bool) error {
	v.logger.Debugw("Delete", "name", name, "dirSync", dirSync)
	// in case we're racing another client
	f := newFile(name, v)
	for i := 0; i <= f.vfs.retries; i++ {

		v.logger.Debugw("Deleting configmaps representing this filename", "name", name)
		cms, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(f.SectorLabels).String()})
		if err != nil {
			v.logger.Errorw("Delete's get cms failed", "err", err)
		}
		v.logger.Debugw("Delete get cms", "cms", cms, "err", err)
		aDeleteFailed := false

		for _, c := range cms.Items {
			err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Delete(context.TODO(), c.Name, metav1.DeleteOptions{})
			if err != nil {
				v.logger.Errorw("Delete failed to delete cm", "cm", c, "err", err)
				aDeleteFailed = true
				continue
			}
			v.logger.Debugw("Deleted configmap", "configmap", c)

		}
		if aDeleteFailed {
			continue
		}

		v.logger.Debugw("Deleting lockfile for this filename", "name", name)
		err = f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Delete(context.TODO(), f.LockFileName(), metav1.DeleteOptions{})
		if kerrors.IsNotFound(err) || err == nil {
			return nil
		} else {
			f.vfs.logger.Error(err)
			continue
		}

		// // err := f.vfs.kc.CoreV1().Namespaces().Delete(context.TODO(), f.vfs.namespace, metav1.DeleteOptions{})
		// v.logger.Debugw("Delete", "name", name, "dirSync", dirSync, "err", err)
		// if kerrors.IsNotFound(err) || err == nil {
		// 	return nil
		// } else {
		// 	f.vfs.logger.Error(err)
		// }
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

	currentLock, err := f.getCurrentLock()
	if err != nil {
		f.vfs.logger.Error(err)
		return false, err
	}
	if currentLock > sqlite3vfs.LockNone {
		// we hold a lock
		return true, nil
	}
	return false, nil
}
