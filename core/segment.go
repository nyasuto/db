package db

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
)

const dir = "./segments"
const int32Size = 4

// 1000 * 1000 = 1MB
const maxSize = int64(10 * 1000 * 1000) // Max size for each segment in bytes

func writeError(err error) error {
	return fmt.Errorf("error writing to file: %s", err)
}

// Segment represents a single log segment
type Segment struct {
	ID       int
	Filepath string
	File     *os.File
	Size     int64
}

// NewSegment creates a new segment
func NewSegment(id int, dir string) (*Segment, error) {
	filepath := filepath.Join(dir, fmt.Sprintf("segment_%d.log", id))
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &Segment{
		ID:       id,
		Filepath: filepath,
		File:     file,
		Size:     0,
	}, nil
}

func loadSegment(id int, dir string) (*Segment, error) {
	filepath := filepath.Join(dir, fmt.Sprintf("segment_%d.log", id))
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &Segment{
		ID:       id,
		Filepath: filepath,
		File:     file,
		Size:     0,
	}, nil
}

func (s *Segment) Read(key string, offset int64) (*string, error) {
	val, _, err := s.readChunk(int64(offset))
	return &val, err
}

func (s *Segment) skipChunk(offset int64) (int64, error) {
	var length int32
	offset -= int64(int32Size)

	// Read the length of the chunk
	buf := make([]byte, int32Size)
	reader := s.File
	_, err := reader.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("Error reading length:", err)
		return 0, err
	}
	length = int32(binary.LittleEndian.Uint32(buf))
	offset -= int64(length)

	return offset, nil
}

func (s *Segment) readChunk(offset int64) (string, int64, error) {
	var length int64
	offset -= int64(int32Size)

	// Read the length of the chunk
	buf := make([]byte, int32Size)

	reader := s.File
	_, err := reader.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("Error reading length:", err)
		return "", 0, err
	}
	length = int64(binary.LittleEndian.Uint32(buf))
	offset -= int64(length)

	// Read the chunk data
	if length > 1000 {
		fmt.Println("Error something bad.")
		return "", 0, err
	}
	buf = make([]byte, length)
	_, err = reader.ReadAt(buf, offset)
	if err != nil {
		fmt.Println("Error reading chunk data:", err)
		return "", 0, err
	}

	return string(buf), offset, nil
}
func (s *Segment) Write(key string, value string) (offset int64, err error) {

	for _, b := range []byte(value) {
		err = binary.Write(s.File, binary.LittleEndian, b)
		if err != nil {
			return 0, writeError(err)
		}
	}

	err = binary.Write(s.File, binary.LittleEndian, int32(len(value)))
	if err != nil {
		return 0, writeError(err)
	}

	offset, _ = s.File.Seek(0, io.SeekCurrent)

	for _, b := range []byte(key) {
		err = binary.Write(s.File, binary.LittleEndian, b)
		if err != nil {
			return 0, writeError(err)
		}
	}
	err = binary.Write(s.File, binary.LittleEndian, int32(len(key)))
	if err != nil {
		return 0, writeError(err)
	}

	stat, _ := s.File.Stat()
	s.Size = stat.Size()

	return offset, nil

}

// Close closes the segment file
func (s *Segment) Close() error {
	return s.File.Close()
}

type index struct {
	SegmentId int
	Offset    int64 // line number of the segment
}

type SegmentManager struct {
	Directory      string
	MaxSegmentSize int64
	Segments       []*Segment
	KeyIndex       map[string]index
	CurrentSegment *Segment
	SegmentCounter int
}

func NewDefaultSegmentManager() (*SegmentManager, error) {
	return NewSegmentManager(dir, maxSize)
}

func (m *SegmentManager) InitializeSegments() bool {
	dir := m.Directory
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err != nil {
			return false
		}
	}
	// scan the directory for existing segment files
	files, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	if len(files) == 0 {
		return false
	}
	segments := []*Segment{}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".log" {
			var id int
			_, err := fmt.Sscanf(file.Name(), "segment_%d.log", &id)
			if err == nil {
				segment, err := loadSegment(id, dir)
				if err != nil {
					return false
				}

				segments = append(segments, segment)
				if id > m.SegmentCounter {
					m.SegmentCounter = id
				}

			}
		}
	}
	// sort the segments by ID
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ID < segments[j].ID
	})

	m.Segments = segments
	if len(m.Segments) > 0 {
		m.CurrentSegment = m.Segments[len(m.Segments)-1]
	}
	err = m.LoadIndex()
	if err != nil {
		log.Fatal(err)
		return false
	}
	return true
}

func (m *SegmentManager) LoadIndex() error {

	for i := m.SegmentCounter - 1; i >= 0; i-- {
		// Read the segment file
		segment := m.Segments[i]
		_, err := segment.File.Seek(0, 0)
		if err != nil {
			return fmt.Errorf("Error seeking to start of segment file:%s", err)
		}
		stat, err := segment.File.Stat()
		if err != nil {
			return err
		}

		offset := int64(stat.Size())

		for offset > 0 {
			key, valOffset, err := segment.readChunk(offset)
			if err != nil {
				return err
			}
			nextKeyOffset, err := segment.skipChunk(valOffset)
			if err != nil {
				return err
			}

			if _, exists := m.KeyIndex[key]; !exists {
				m.KeyIndex[key] = index{SegmentId: i + 1, Offset: valOffset}
			}
			offset = nextKeyOffset

		}
	}
	return nil
}
func (m *SegmentManager) CloseAll() {
	for _, segment := range m.Segments {
		segment.Close()
	}
}

// NewSegmentManager initializes the segment manager
func NewSegmentManager(directory string, maxSize int64) (*SegmentManager, error) {
	manager := &SegmentManager{
		Directory:      directory,
		MaxSegmentSize: maxSize,
		Segments:       []*Segment{},
		SegmentCounter: 0,
		KeyIndex:       make(map[string]index),
	}

	if !manager.InitializeSegments() {
		// if no existing DB files
		err := manager.createSegment()
		if err != nil {
			return nil, err
		}
	}

	return manager, nil
}

// createSegment creates a new segment
func (m *SegmentManager) createSegment() error {
	//if m.CurrentSegment != nil {
	//	m.CurrentSegment.Close()
	//}

	m.SegmentCounter++
	segment, err := NewSegment(m.SegmentCounter, m.Directory)
	if err != nil {
		return err
	}

	m.Segments = append(m.Segments, segment)
	m.CurrentSegment = segment
	return nil
}

// Write writes a key-value pair to the current segment
func (m *SegmentManager) Write(key, value string) (err error) {
	if m.CurrentSegment.Size >= m.MaxSegmentSize {
		err := m.createSegment()
		if err != nil {
			return err
		}
	}

	offset, err := m.CurrentSegment.Write(key, value)
	if err != nil {
		return err
	}

	m.KeyIndex[key] = index{SegmentId: m.CurrentSegment.ID, Offset: offset}
	return nil

}

// Read a value by key
func (m *SegmentManager) Read(key string) (value string, err error) {
	if index, ok := m.KeyIndex[key]; ok {
		segment := m.Segments[index.SegmentId-1]
		val, err := segment.Read(key, index.Offset)
		if err != nil {
			return "", err
		}
		if val != nil {
			return *val, nil
		}
	}

	return "", fmt.Errorf("Key (%s) Not found", key)
}
