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
	return pos - (pos % SectorSize)
}

func (f *file) deleteSector(sectorOffset int64) error {
	n := sectorNameFromSectorOffset(sectorOffset)
	err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).Delete(context.TODO(), n, metav1.DeleteOptions{})
	return err
}

func (f *file) writeSector(s *sector) error {
	sectorName := sectorNameFromSectorOffset(s.offset)
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

func sectorNameFromSectorOffset(sectorOffset int64) string {
	sectorName := fmt.Sprintf("%d", sectorOffset)
	return sectorName
}

func (f *file) getSector(sectorOffset int64) (*sector, error) {
	sectorName := sectorNameFromSectorOffset(sectorOffset)
	cm, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).Get(context.TODO(), sectorName, metav1.GetOptions{})

	if err != nil {
		f.vfs.logger.Error(err)
		return nil, err
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
	cms, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(SectorLabel).String()})

	if err != nil {
		f.vfs.logger.Error(err)
		return nil, err
	}
	sectorNo := len(cms.Items) - 1

	return f.getSector(int64(sectorNo))

}

func (f *file) getSectorRange(firstSector, lastSector int64) ([]*sector, error) {

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
