# Word Count with Memory Limit (Go)

This Go program counts word frequencies from an input text file, with each line containing a single word. It is designed to handle large files efficiently by **limiting the number of words stored in memory** and using disk-based temporary storage when necessary.

---

## 🧠 Strategy Overview

### 1. **Two-Phase Process**
The program uses a two-phase process to count words:

#### **Phase 1: Counting and Flushing**
- Reads the input file line by line.
- Stores word counts in an in-memory map.
- Once the number of unique words reaches a user-defined limit (`MAX_WORDS_IN_MEMORY`), the map is flushed to a **sorted temporary file**.
- This repeats until the full input is processed.

#### **Phase 2: Multi-Pass K-Way Merge**
- Temporary files are merged in **batches**, with each batch containing at most `MAX_WORDS_IN_MEMORY` files.
- A **k-way merge** with a **min-heap (priority queue)** is used to efficiently merge sorted files.
- During merging, memory is limited to holding no more than `MAX_WORDS_IN_MEMORY` words.
- Intermediate merged files are generated if needed until only one final output file remains.

---

## ✅ Features

- Scalable to very large datasets.
- Does **not load all data into memory**.
- Output is sorted lexicographically.
- **Tab-separated** output in `output.tsv`.

---

## 🚀 Usage

### 📦 Run

```bash
go run cmd/main.go 10 input.txt
