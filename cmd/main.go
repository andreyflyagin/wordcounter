package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

var MAX_WORDS_IN_MEMORY int

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: wordcount <max_words_in_memory> <input_file>")
		os.Exit(1)
	}

	var err error
	MAX_WORDS_IN_MEMORY, err = strconv.Atoi(os.Args[1])
	if err != nil || MAX_WORDS_IN_MEMORY <= 0 {
		fmt.Println("Invalid MAX_WORDS_IN_MEMORY:", os.Args[1])
		os.Exit(1)
	}

	inputFile := os.Args[2]
	outputFile := "output.tsv"

	tempFiles, err := processInputFile(inputFile)
	if err != nil {
		panic(err)
	}

	finalFile, err := mergeInBatches(tempFiles)
	if err != nil {
		panic(err)
	}

	err = os.Rename(finalFile, outputFile)
	if err != nil {
		panic(err)
	}

	for _, f := range tempFiles {
		os.Remove(f)
	}
}

// ------------------- Input Phase -------------------

func processInputFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	wordCount := make(map[string]int)
	var tempFiles []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		word := strings.TrimSpace(scanner.Text())
		if word == "" {
			continue
		}
		wordCount[word]++
		if len(wordCount) >= MAX_WORDS_IN_MEMORY {
			tmp, err := flushToTempFile(wordCount)
			if err != nil {
				return nil, err
			}
			tempFiles = append(tempFiles, tmp)
			wordCount = make(map[string]int)
		}
	}

	if len(wordCount) > 0 {
		tmp, err := flushToTempFile(wordCount)
		if err != nil {
			return nil, err
		}
		tempFiles = append(tempFiles, tmp)
	}
	return tempFiles, nil
}

func flushToTempFile(wordCount map[string]int) (string, error) {
	tmpFile, err := os.CreateTemp("", "wordcount_*.tmp")
	if err != nil {
		return "", err
	}
	defer tmpFile.Close()

	words := make([]string, 0, len(wordCount))
	for word := range wordCount {
		words = append(words, word)
	}
	sort.Strings(words)

	writer := bufio.NewWriter(tmpFile)
	for _, word := range words {
		fmt.Fprintf(writer, "%s\t%d\n", word, wordCount[word])
	}
	writer.Flush()
	return tmpFile.Name(), nil
}

// ------------------- K-Way Merge with Batching -------------------

func mergeInBatches(files []string) (string, error) {
	for len(files) > 1 {
		var nextRoundFiles []string

		for i := 0; i < len(files); i += MAX_WORDS_IN_MEMORY {
			end := i + MAX_WORDS_IN_MEMORY
			if end > len(files) {
				end = len(files)
			}
			batch := files[i:end]
			merged, err := mergeBatch(batch)
			if err != nil {
				return "", err
			}
			nextRoundFiles = append(nextRoundFiles, merged)

			for _, f := range batch {
				os.Remove(f)
			}
		}
		files = nextRoundFiles
	}

	return files[0], nil
}

func mergeBatch(tempFiles []string) (string, error) {
	readers := make([]*bufio.Scanner, len(tempFiles))
	files := make([]*os.File, len(tempFiles))
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()

	h := &fileEntryHeap{}
	heap.Init(h)

	for i, tempFile := range tempFiles {
		f, err := os.Open(tempFile)
		if err != nil {
			return "", err
		}
		files[i] = f
		scanner := bufio.NewScanner(f)
		readers[i] = scanner

		if scanner.Scan() {
			word, count := parseLine(scanner.Text())
			heap.Push(h, &fileEntry{word, count, i})
		}
	}

	tmpOutFile, err := os.CreateTemp("", "merged_*.tmp")
	if err != nil {
		return "", err
	}
	writer := bufio.NewWriter(tmpOutFile)
	defer func() {
		writer.Flush()
		tmpOutFile.Close()
	}()

	wordBuffer := make(map[string]int)

	for h.Len() > 0 {
		entry := heap.Pop(h).(*fileEntry)

		if _, ok := wordBuffer[entry.word]; !ok && len(wordBuffer) >= MAX_WORDS_IN_MEMORY {
			if err := flushBufferToWriter(wordBuffer, writer); err != nil {
				return "", err
			}
			wordBuffer = make(map[string]int)
		}

		wordBuffer[entry.word] += entry.count

		scanner := readers[entry.fileIdx]
		if scanner.Scan() {
			word, count := parseLine(scanner.Text())
			heap.Push(h, &fileEntry{word, count, entry.fileIdx})
		}
	}

	if len(wordBuffer) > 0 {
		if err := flushBufferToWriter(wordBuffer, writer); err != nil {
			return "", err
		}
	}

	return tmpOutFile.Name(), nil
}

// ------------------- Utility -------------------

type fileEntry struct {
	word    string
	count   int
	fileIdx int
}

type fileEntryHeap []*fileEntry

func (h fileEntryHeap) Len() int           { return len(h) }
func (h fileEntryHeap) Less(i, j int) bool { return h[i].word < h[j].word }
func (h fileEntryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *fileEntryHeap) Push(x interface{}) {
	*h = append(*h, x.(*fileEntry))
}

func (h *fileEntryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func parseLine(line string) (string, int) {
	parts := strings.SplitN(line, "\t", 2)
	if len(parts) != 2 {
		return "", 0
	}
	count, _ := strconv.Atoi(parts[1])
	return parts[0], count
}

func flushBufferToWriter(buffer map[string]int, writer *bufio.Writer) error {
	words := make([]string, 0, len(buffer))
	for word := range buffer {
		words = append(words, word)
	}
	sort.Strings(words)

	for _, word := range words {
		_, err := fmt.Fprintf(writer, "%s\t%d\n", word, buffer[word])
		if err != nil {
			return err
		}
	}
	return writer.Flush()
}
