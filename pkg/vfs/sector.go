package vfs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/psanford/sqlite3vfs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

var sectorNotFoundErr = errors.New("sector not found")

type sector struct {
	offset int64
	data   []byte
}

func (f *file) sectorForPos(pos int64) int64 {

	s := pos - (pos % SectorSize)
	f.vfs.logger.Debugw("sectorForPos", "pos", pos, "sector", s)

	return s
}

func (f *file) deleteSector(sectorOffset int64) error {
	n := f.sectorNameFromSectorOffset(sectorOffset)
	err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).Delete(context.TODO(), n, metav1.DeleteOptions{})
	f.vfs.logger.Debugw("deleteSector", "sectorOffset", sectorOffset, "err", err)

	return err
}

func (f *file) writeSector(s *sector) error {
	f.vfs.logger.Debugw("writeSector", "sector", s)
	sectorName := f.sectorNameFromSectorOffset(s.offset)
	b64Data := make([]byte, base64.StdEncoding.EncodedLen(len(s.data)))
	base64.StdEncoding.Encode(b64Data, s.data)
	//map[string][]byte]{"sector": "b64Data"}
	// binaryDataField :=  map[string][]byte{"sector": b64Data}
	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      sectorName,
			Namespace: f.namespaceName(),
			Labels:    SectorLabel,
		},
		BinaryData: map[string][]byte{"sector": b64Data},
	}
	_, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).Create(context.TODO(), cm, metav1.CreateOptions{})
	if err != nil {
		f.vfs.logger.Error(err)
		return err
	}
	return nil
}

func (f *file) sectorNameFromSectorOffset(sectorOffset int64) string {

	// if sectorOffset == -1 {
	// 	last, err := f.getLastSector()

	// }
	sectorName := fmt.Sprintf("%d", sectorOffset)
	f.vfs.logger.Debugw("sectorNameFromSectorOffset", "sectorOffset", sectorOffset, "sectorName", sectorName)

	return sectorName
}

func (f *file) getSector(sectorOffset int64) (*sector, error) {
	f.vfs.logger.Debugw("getSector", "sectorOffset", sectorOffset)
	sectorName := f.sectorNameFromSectorOffset(sectorOffset)
	cm, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).Get(context.TODO(), sectorName, metav1.GetOptions{})

	if err != nil {
		f.vfs.logger.Error(err)
		return nil, sqlite3vfs.CantOpenError
	}

	// Make a new function, and inverse
	sectorData := make([]byte, base64.StdEncoding.DecodedLen(len(cm.BinaryData["sector"])))
	n, err := base64.StdEncoding.Decode(sectorData, []byte(cm.BinaryData["sector"]))
	// sectorData, err := f.bytesFromB32Byte(cm.BinaryData["sector"])
	if err != nil {
		f.vfs.logger.Error(err)
		return nil, sqlite3vfs.IOErrorRead
	}
	sectorData = sectorData[:n]

	s := sector{
		offset: sectorOffset,
		data:   sectorData,
	}

	return &s, nil
}

func (f *file) getLastSector() (*sector, error) {
	f.vfs.logger.Debugw("getLastSector")

	cms, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(SectorLabel).String()})
	f.vfs.logger.Debugw("getLastSector", "sector configmaps", cms, "err", err)


	if err != nil {
		f.vfs.logger.Error(err)
		return nil, err
	}

	sectorNo := len(cms.Items) - 1

	f.vfs.logger.Debugw("getLastSector", "sectorNo", sectorNo)

	return f.getSector(int64(sectorNo))

}

func (f *file) getSectorRange(firstSector, lastSector int64) ([]*sector, error) {
	f.vfs.logger.Debugw("getSectorRange", "firstSector", firstSector, "lastSector", lastSector)

	if firstSector == lastSector {
		sect, err := f.getSector(firstSector)
		if err == sectorNotFoundErr {
			return nil, nil
		} else if err != nil {
			return nil, err
		}
		return []*sector{sect}, nil
	}
	sectors := make([]*sector, (lastSector - firstSector))

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
