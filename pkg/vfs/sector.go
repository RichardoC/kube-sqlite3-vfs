package vfs

import (
	"context"
	"errors"

	"github.com/psanford/sqlite3vfs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

var sectorNotFoundErr = errors.New("sector not found")

type sector struct {
	offset int64
	data   []byte
}

func (f *file) getSector(sectorOffset int64) (*sector, error) {
	cm, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).Get(context.TODO(), LockFileName, metav1.GetOptions{})

	if err != nil {
		f.vfs.logger.Error(err)
		return nil, err
	}

	sectorData, err := f.stringFromB32Byte(cm.BinaryData["sector"])
	if err != nil {
		f.vfs.logger.Error(err)
		return nil, sqlite3vfs.IOErrorRead
	}

	s := sector{
		offset: sectorOffset,
		data:   []byte(sectorData),
	}

	return &s, nil
}

func (f *file) getLastSector() (*sector, error) {
	cms, err := f.vfs.kc.CoreV1().ConfigMaps(f.namespaceName()).List(context.TODO(), metav1.ListOptions{LabelSelector: labels.SelectorFromSet(SectorLabel).String()})

	if err != nil {
		f.vfs.logger.Error(err)
		return nil, err
	}
	sectorNo := len(cms.Items) -1

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
