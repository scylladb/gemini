package unmarshal

import (
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gemini/pkg/murmur"
	"sort"
)

// writeToHash get hash for protocol V2 and older
func writeToHash(proto byte, pathname Path, info gocql.TypeInfo, h *murmur.Hash, data []byte) error {
	switch info.Type() {
	case gocql.TypeList, gocql.TypeSet:
		return writeListToHash(proto, pathname, info, h, data)
	case gocql.TypeMap:
		return writeMapToHash(proto, pathname, info, h, data)
	case gocql.TypeTuple:
		return writeTupleToHash(proto, pathname, info, h, data)
	case gocql.TypeUDT:
		return writeUDTToHash(proto, pathname, info, h, data)
	default:
		h.Write2(data...)
		return nil
	}
}

// GetColumnHash get hash for protocol V2 and older
func GetColumnHash(info gocql.TypeInfo, data []byte) (uint64, error) {
	hash := murmur.Hash{}
	proto := info.Version()
	err := writeToHash(proto, nil, info, &hash, data)
	if err != nil {
		return 0, err
	}
	return hash.Sum64(), nil
}

func writeCollectionLength(protoVersion byte, h *murmur.Hash, data []byte) {
	if protoVersion > protoVersion2 {
		h.Write2(data[:4]...)
	}
	h.Write2(0, 0, data[1], data[0])
}

func readListElement(protoVersion byte, data []byte) (out []byte, size int, err error) {
	var p int
	size, p, err = readCollectionSize(protoVersion, data)
	if err != nil {
		return nil, 0, errors.Wrap(err, "unmarshal: failed to read collection element length")
	}
	data = data[p:]
	if len(data) < size {
		return nil, 0, unmarshalErrorf("unmarshal list: unexpected eof")
	}
	return data, size, nil
}

func uint64ToBytes(val uint64) []byte {
	return []byte{byte(val >> 56), byte(val >> 48), byte(val >> 40), byte(val >> 32), byte(val >> 24), byte(val >> 16), byte(val >> 8), byte(val)}
}

func writeListToHash(proto byte, pathname Path, info gocql.TypeInfo, h *murmur.Hash, data []byte) error {
	listInfo, ok := info.(gocql.CollectionType)
	if !ok {
		return errors.Errorf("%s can't unmarshal none collection type into list", pathname.String())
	}
	n, p, err := readCollectionSize(proto, data)
	if err != nil {
		return errors.Wrapf(err, "%s failed to read list lenght", pathname.String())
	}
	writeCollectionLength(proto, h, data)

	data = data[p:]
	var m int
	for i := 0; i < n; i++ {
		if len(data) == 0 {
			break
		}
		elemPath := pathname.Add(i)
		data, m, err = readListElement(proto, data)
		if err != nil {
			return errors.Wrapf(err, "%s failed to read oracle data", pathname.String())
		}
		err = writeToHash(proto, elemPath, listInfo.Elem, h, data[:m])
		if err != nil {
			return err
		}
		data = data[m:]
	}
	return nil
}

func writeMapToHash(proto byte, pathname Path, info gocql.TypeInfo, h *murmur.Hash, data []byte) error {
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

	if len(data) == 0 {
		return nil
	}
	n, p, err := readCollectionSize(proto, data)
	if err != nil {
		return err
	}
	writeCollectionLength(proto, h, data)
	data = data[p:]

	records := make([]*mapRecord, n)
	for i := 0; i < n; i++ {
		records[i] = &mapRecord{}
		m, p, err := readCollectionSize(proto, data)
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
		err = writeToHash(proto, pathname.Add(i), mapInfo.Key, &localHash, data[:m])
		if err != nil {
			return err
		}

		records[i].keyHash = localHash.Sum64()
		records[i].keyData = data[:m]
		data = data[m:]

		m, p, err = readCollectionSize(proto, data)
		if err != nil {
			return err
		}

		if len(data) < m {
			return unmarshalErrorf("unmarshal map: unexpected eof")
		}
		records[i].valLenData = data[:m]
		data = data[m:]

		localHash.Reset()
		err = writeToHash(proto, pathname.Add(i), mapInfo.Key, &localHash, data[:m])
		if err != nil {
			return err
		}
		records[i].valHash = localHash.Sum64()
		data = data[m:]
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].keyHash < records[j].keyHash
	})

	for i := 0; i < n; i++ {
		err = writeToHash(proto, pathname.Add(i), mapInfo.Key, h, records[i].keyData)
		if err != nil {
			return err
		}
		err = writeToHash(proto, pathname.Add(i), mapInfo.Elem, h, records[i].valData)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeTupleToHash(proto byte, pathname Path, info gocql.TypeInfo, h *murmur.Hash, data []byte) error {
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
		err := writeToHash(proto, pathname.Add(i), elem, h, p)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeUDTToHash(proto byte, pathname Path, info gocql.TypeInfo, h *murmur.Hash, data []byte) error {
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

		err := writeToHash(proto, pathname.Add(i), elem.Type, &localHash, p)
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
		err := writeToHash(proto, pathname.Add(i), elem.Type, h, records[i].valData)
		if err != nil {
			return err
		}
	}

	return nil
}
