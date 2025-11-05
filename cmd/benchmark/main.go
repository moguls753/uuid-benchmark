package main

import (
"fmt"
"log"
"math/rand"
"time"

"github.com/google/uuid"
"github.com/oklog/ulid/v2"
)

func main() {
fmt.Println("UUID Benchmark Tool")
fmt.Println("===================")
fmt.Println()

numExamples := 5

fmt.Println("UUIDv1 (Time-based with MAC address):")
fmt.Println("--------------------------------------")
for i := 0; i < numExamples; i++ {
	v1, err := uuid.NewUUID()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d. %s\n", i+1, v1.String())
	time.Sleep(1 * time.Millisecond)
}
fmt.Println()

fmt.Println("UUIDv4 (Random):")
fmt.Println("----------------")
for i := 0; i < numExamples; i++ {
	v4 := uuid.New()
	fmt.Printf("%d. %s\n", i+1, v4.String())
}
fmt.Println()

fmt.Println("UUIDv7 (Timestamp-sortable):")
fmt.Println("-----------------------------")
for i := 0; i < numExamples; i++ {
	v7, err := uuid.NewV7()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%d. %s\n", i+1, v7.String())
	time.Sleep(1 * time.Millisecond)
}
fmt.Println()

fmt.Println("ULID (Lexicographically sortable):")
fmt.Println("-----------------------------------")
entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
for i := 0; i < numExamples; i++ {
	ul := ulid.MustNew(ulid.Timestamp(time.Now()), entropy)
	fmt.Printf("%d. %s\n", i+1, ul.String())
	time.Sleep(1 * time.Millisecond)
}
fmt.Println()

fmt.Println("INT8 / BIGSERIAL (Sequential integers - reference):")
fmt.Println("----------------------------------------------------")
var startID int64 = 1000000
for i := 0; i < numExamples; i++ {
	fmt.Printf("%d. %d (0x%016x)\n", i+1, startID+int64(i), startID+int64(i))
}
fmt.Println()
}
