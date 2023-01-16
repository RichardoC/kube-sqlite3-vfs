package vfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/psanford/sqlite3vfs"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

var sectorNotFoundErr = errors.New("sector not found")

type Sector struct {
	Index  int64
	Data   []byte
	Labels map[string]string
	// Namespace string
}

func (f *file) sectorForPos(pos int64) int64 {

	s := (pos / SectorSize)
	f.vfs.logger.Debugw("sectorForPos", "pos", pos, "sector", s)

	return s
}

func (f *file) deleteSector(sectorIndex int64) error {
	n := f.sectorNameFromSectorIndex(sectorIndex)
	err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Delete(context.TODO(), n, metav1.DeleteOptions{})
	f.vfs.logger.Debugw("deleteSector", "sectorIndex", sectorIndex, "err", err)

	return err
}

func (f *file) WriteSector(s *Sector) error {
	f.vfs.logger.Debugw("writeSector", "sectorIndex", s.Index)
	sectorName := f.sectorNameFromSectorIndex(s.Index)
	// b64Data := make([]byte, SectorSize)
	// base64.StdEncoding.Encode(b64Data, s.data)
	//map[string][]byte]{"sector": "b64Data"}
	// binaryDataField :=  map[string][]byte{"sector": b64Data}
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      sectorName,
			Namespace: f.vfs.namespace,
			Labels:    f.SectorLabels,
		},
		BinaryData: map[string][]byte{"sector": s.Data},
		Data:       map[string]string{"filename": f.RawName},
	}
	_, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Create(context.TODO(), cm, metav1.CreateOptions{})
	if kerrors.IsAlreadyExists(err) {
		_, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
		if err != nil {
			f.vfs.logger.Error(err)
			return err
		}
		return nil
	} else if err != nil {
		f.vfs.logger.Error(err)
		return err
	}
	return nil
}

func (f *file) sectorNameFromSectorIndex(sectorIndex int64) string {

	// if sectorIndex == -1 {
	// 	last, err := f.getLastSector()

	// }

	sectorName := fmt.Sprintf("%s-%d", f.b32ByteFromString(f.RawName), sectorIndex)
	f.vfs.logger.Debugw("sectorNameFromSectorIndex", "sectorIndex", sectorIndex, "sectorName", sectorName)

	return sectorName
}

func (f *file) getSector(sectorIndex int64) (*Sector, error) {
	f.vfs.logger.Debugw("getSector", "sectorIndex", sectorIndex)
	sectorName := f.sectorNameFromSectorIndex(sectorIndex)
	cm, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Get(context.TODO(), sectorName, metav1.GetOptions{})
	f.vfs.logger.Debugw("getSector", "sectorIndex", sectorIndex, "err", err)

	// Make an empty sector if it doesn't exist
	// Since we read then write
	if kerrors.IsNotFound(err) {

		err := f.WriteSector(&Sector{Index: sectorIndex})
		if err != nil {
			f.vfs.logger.Error(err)
			return nil, err
		}
		// cm, err = f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).Get(context.TODO(), sectorName, metav1.GetOptions{})
		// if err != nil {
		// 	f.vfs.logger.Error(err)
		// 	return nil, err
		// }

	} else if err != nil {
		f.vfs.logger.Error(err)
		return nil, sqlite3vfs.IOErrorShortRead
	}

	// Make a new function, and inverse
	sectorData := make([]byte, SectorSize)
	n := copy(sectorData, cm.BinaryData["sector"])
	// n, err := base64.StdEncoding.Decode(sectorData, cm.BinaryData["sector"])
	// sectorData, err := f.bytesFromB32Byte(cm.BinaryData["sector"])
	// if err != nil {
	// 	f.vfs.logger.Error(err)
	// 	return nil, sqlite3vfs.IOErrorRead
	// }
	sectorData = sectorData[:n]

	s := Sector{
		Index: sectorIndex,
		Data:  sectorData,
	}

	return &s, nil
}

func (f *file) getLastSector() (*Sector, error) {
	f.vfs.logger.Debugw("getLastSector")

	cms, err := f.vfs.kc.CoreV1().ConfigMaps(f.vfs.namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(f.SectorLabels).String()})
	f.vfs.logger.Debugw("getLastSector", "f.RawName", f.RawName, "len(configmaps)", len(cms.Items), "err", err, "f.sectorLabels", f.SectorLabels)

	if err != nil {
		f.vfs.logger.Error(err)
		return nil, err
	}
	if len(cms.Items) == 0 {
		f.vfs.logger.Debugw("getLastSector failed to find any sectors", "f", f, "cms", cms)
		return nil, errors.New("failed to find any existing sectors")

	}

	sectorIndex := len(cms.Items) - 1

	f.vfs.logger.Debugw("getLastSector", "sectorIndex", sectorIndex)

	return f.getSector(int64(sectorIndex))

}

func (f *file) getSectorRange(firstSector, lastSector int64) ([]*Sector, error) {
	f.vfs.logger.Debugw("getSectorRange", "firstSector", firstSector, "lastSector", lastSector)

	if firstSector == lastSector {
		sect, err := f.getSector(firstSector)
		if err == sectorNotFoundErr {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		return []*Sector{sect}, nil
	}
	sectors := make([]*Sector, (lastSector - firstSector))

	for i := firstSector; i <= lastSector; i++ {
		thisSector, err := f.getSector(i)
		if err != nil {
			f.vfs.logger.Error(err)
			return nil, sqlite3vfs.IOErrorRead
		}
		sectors[i] = thisSector

	}

	return sectors, nil
}
