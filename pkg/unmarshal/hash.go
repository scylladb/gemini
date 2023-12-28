package unmarshal

import (
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gemini/pkg/murmur"
	"hash"
	"sort"
)

func writeCollectionLength(protoVersion byte, h hash.Hash64, data []byte) error {
	if protoVersion > protoVersion2 {
		_, err := h.Write(data[:4])
		if err != nil {
			return err
		}
		return nil
	}
	var buff [4]byte
	copy(buff[2:], data[:2])
	_, err := h.Write(buff[:])
	if err != nil {
		return err
	}
	return nil
}

func getHashList(pathname Path, info gocql.TypeInfo, h hash.Hash64, data []byte) error {
	listInfo, ok := info.(gocql.CollectionType)
	if !ok {
		return errors.Errorf("%s can't unmarshal none collection type into list", pathname.String())
	}
	pversion := listInfo.NativeType.Version()
	n, p, err := readCollectionSize(pversion, data)
	if err != nil {
		return errors.Wrapf(err, "%s failed to read list lenght", pathname.String())
	}
	err = writeCollectionLength(pversion, h, data)
	if err != nil {
		return errors.Wrapf(err, "%s failed to write to hash", pathname.String())
	}

	data = data[p:]
	var m int
	for i := 0; i < n; i++ {
		if len(data) == 0 {
			break
		}
		elemPath := pathname.Add(i)
		data, m, err = readListElement(pversion, data)
		if err != nil {
			return errors.Wrapf(err, "%s failed to read oracle data", pathname.String())
		}
		err = getHash(elemPath, listInfo.Elem, h, data[:m])
		if err != nil {
			return err
		}
		data = data[m:]
	}
	return nil
}

func getHashMap(pathname Path, info gocql.TypeInfo, h hash.Hash64, data []byte) error {
	type mapRecord struct {
		keyHash    uint64
		keyLenData []byte
		keyData    []byte
		valHash    uint64
		valData    []byte
		valLenData []byte
	}

	mapInfo, ok := info.(gocql.CollectionType)
	if !ok {
		return errors.Errorf("%s can't unmarshal none collection type into map", pathname.String())
	}
	pversion := mapInfo.NativeType.Version()

	if len(data) == 0 {
		return nil
	}
	n, p, err := readCollectionSize(pversion, data)
	if err != nil {
		return err
	}
	err = writeCollectionLength(pversion, h, data)
	if err != nil {
		return errors.Wrapf(err, "%s failed to write to hash", pathname.String())
	}
	data = data[p:]

	records := make([]*mapRecord, n)
	for i := 0; i < n; i++ {
		records[i] = &mapRecord{}
		m, p, err := readCollectionSize(pversion, data)
		if err != nil {
			return err
		}

		if len(data) < p {
			return unmarshalErrorf("unmarshal map: unexpected eof")
		}
		records[i].keyLenData = data[:p]
		data = data[p:]

		if len(data) < m {
			return unmarshalErrorf("unmarshal map: unexpected eof")
		}

		localHash := murmur.Hash{}
		getHash(pathname.Add(i), mapInfo.Key, &localHash, data[:m])

		records[i].keyHash = localHash.Sum64()
		records[i].keyData = data[:m]
		data = data[m:]

		m, p, err = readCollectionSize(pversion, data)
		if err != nil {
			return err
		}

		if len(data) < m {
			return unmarshalErrorf("unmarshal map: unexpected eof")
		}
		records[i].valLenData = data[:m]
		data = data[m:]

		localHash.Reset()
		getHash(pathname.Add(i), mapInfo.Key, &localHash, data[:m])
		records[i].valHash = localHash.Sum64()
		data = data[m:]
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].keyHash < records[j].keyHash
	})

	for i := 0; i < n; i++ {
		err = getHash(pathname.Add(i), mapInfo.Key, h, records[i].keyData)
		if err != nil {
			return err
		}
		err = getHash(pathname.Add(i), mapInfo.Elem, h, records[i].valData)
		if err != nil {
			return err
		}
	}

	return nil
}

func getHashOldTuple(pathname Path, info gocql.TypeInfo, h hash.Hash64, data []byte) error {
	tupleInfo, ok := info.(gocql.TupleTypeInfo)
	if !ok {
		return errors.Errorf("%s can't unmarshal none tuple type into tuple", pathname.String())
	}

	for i, elem := range tupleInfo.Elems {
		// each element inside data is a [bytes]
		var p []byte
		if len(data) >= 4 {
			p, data = readBytes(data)
		}
		err := getHash(pathname.Add(i), elem, h, p)
		if err != nil {
			return err
		}
	}
	return nil
}

func getHashOldUDT(pathname Path, info gocql.TypeInfo, h hash.Hash64, data []byte) error {
	udtInfo, ok := info.(gocql.UDTTypeInfo)
	if !ok {
		return errors.Errorf("%s can't unmarshal none tuple type into tuple", pathname.String())
	}

	type udtRecord struct {
		key     string
		valHash uint64
		valData []byte
	}

	localHash := murmur.Hash{}
	records := make([]*udtRecord, len(udtInfo.Elements))
	for i, elem := range udtInfo.Elements {
		var p []byte
		if len(data) >= 4 {
			p, data = readBytes(data)
		}
		records[i] = &udtRecord{
			key:     elem.Name,
			valData: p,
		}

		err := getHash(pathname.Add(i), elem.Type, &localHash, p)
		if err != nil {
			return err
		}
		records[i].valHash = localHash.Sum64()
		localHash.Reset()
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].key < records[j].key
	})

	for i, elem := range udtInfo.Elements {
		err := getHash(pathname.Add(i), elem.Type, h, records[i].valData)
		if err != nil {
			return err
		}
	}

	return nil
}
