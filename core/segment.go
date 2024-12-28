package db

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

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
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

func (s *Segment) ReadByLine(key string, lineNumber int) (*string, error) {
	_, err := s.File.Seek(0, 0)
	if err != nil {
		return nil, err
	}
	buff, err := io.ReadAll(s.File)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(buff), "\n")

	parts := strings.SplitN(lines[lineNumber], "=", 2)
	if len(parts) == 2 && parts[0] == key {
		return &parts[1], nil
	}

	return nil, nil
}
func (s *Segment) Read(key string) (*string, error) {
	_, err := s.File.Seek(0, 0)
	if err != nil {
		return nil, err
	}
	buff, err := io.ReadAll(s.File)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(buff), "\n")

	for _, line := range lines {

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 && parts[0] == key {
			return &parts[1], nil
		}
	}

	return nil, nil
}

// Write writes a key-value pair to the segment
func (s *Segment) Write(key, value string) error {
	entry := fmt.Sprintf("%s=%s\n", key, value)
	n, err := s.File.WriteString(entry)
	if err != nil {
		return err
	}
	s.Size += int64(n)
	return nil
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

const dir = "./segments"

// 1000 * 1000 = 1MB
const maxSize = int64(1000 * 1000) // Max size for each segment in bytes

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
	m.KeyIndex = make(map[string]index)
	for i := m.SegmentCounter - 1; i >= 0; i-- {
		// Read the segment file
		segment := m.Segments[i]
		_, err := segment.File.Seek(0, 0)
		if err != nil {
			return fmt.Errorf("Error seeking to start of segment file:%s", err)
		}

		scanner := bufio.NewScanner(segment.File)
		offset := int64(0)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				m.KeyIndex[parts[0]] = index{SegmentId: i, Offset: offset}
			}
			offset++
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("Error scanning segment file:%s", err)
		}
	}
	return nil
}

// NewSegmentManager initializes the segment manager
func NewSegmentManager(directory string, maxSize int64) (*SegmentManager, error) {
	manager := &SegmentManager{
		Directory:      directory,
		MaxSegmentSize: maxSize,
		Segments:       []*Segment{},
		SegmentCounter: 0,
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
	if m.CurrentSegment != nil {
		m.CurrentSegment.Close()
	}

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
func (m *SegmentManager) Write(key, value string) error {
	if m.CurrentSegment.Size >= m.MaxSegmentSize {
		err := m.createSegment()
		if err != nil {
			return err
		}
	}

	return m.CurrentSegment.Write(key, value)
}

// Read a value by key
func (m *SegmentManager) Read(key string) (value string, err error) {
	if index, ok := m.KeyIndex[key]; ok {
		segment := m.Segments[index.SegmentId]
		val, err := segment.ReadByLine(key, int(index.Offset))
		if err != nil {
			return "", err
		}
		if val != nil {
			return *val, nil
		}
	}

	fmt.Printf("Key (%s) not found in cache\n", key)

	for i := m.SegmentCounter - 1; i >= 0; i-- {
		// Read the segment file
		val, err := m.Segments[i].Read(key)
		if err != nil {
			return "", err
		}
		if val != nil {
			// fmt.Printf("Key (%s) found in segment %s\n", key, m.Segments[i].Filepath)
			return *val, nil
		}
	}
	return "", fmt.Errorf("Key (%s) Not found", key)
}
