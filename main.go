package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Coordinates struct {
	Lat  float64 `json:"lat"`
	Long float64 `json:"long"`
}

// Location struct for MongoDB
type Location struct {
	PlaceID      string       `json:"place_id"`
	Address      string       `json:"address"`
	Coord        *Coordinates `json:"coordinates"`
	Suggesters   []any        `json:"suggesters"`
	Reviewers    []any        `json:"reviewers"`
	IsMerged     bool         `json:"is_merged"`
	IsSuggested  bool         `json:"is_suggested"`
	IsReviewed   bool         `json:"is_reviewed"`
	CorrectCount int          `json:"correct_count"`
}

// File to store the last processed PlaceID
var progressFile = "_progress.txt"

// CSV processing and MongoDB insertion
func processCSV(csvFile string, mongoURI string, dbName string, collectionName string) error {
	// Connect to MongoDB
	clientOpts := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(context.Background(), clientOpts)
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())

	collection := client.Database(dbName).Collection(collectionName)

	// Open CSV file
	file, err := os.Open(csvFile)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Retrieve last processed PlaceID
	lastProcessedID, err := getLastProcessedPlaceID()
	if err != nil {
		return err
	}

	// Track progress
	totalRecords := 0
	progressBar := pb.New(totalRecords).Set(pb.Bytes, true).SetWidth(27)
	progressBar.Start()

	batchSize := 1000
	var batch []interface{}
	startProcessing := lastProcessedID == ""

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				// End of file
				go func() {
					time.Sleep(10 * time.Millisecond)
					fmt.Printf("\n> GOT EOF \n")
				}()

				break
			}
			return err
		}

		if !startProcessing && record[0] == lastProcessedID {
			startProcessing = true
			continue
		}

		if !startProcessing {
			continue
		}

		// Skip non-Bangladesh locations
		if record[4] != "Bangladesh" {
			progressBar.Increment()
			continue
		}

		location := Location{
			PlaceID: record[0],
			Address: record[2],
			Coord: &Coordinates{
				Lat:  parseFloat(record[9]),
				Long: parseFloat(record[10]),
			},
			IsMerged:    false,
			IsSuggested: false,
			IsReviewed:  false,
			Suggesters:  nil,
			Reviewers:   nil,
		}

		batch = append(batch, location)

		if len(batch) >= batchSize {
			_, err := collection.InsertMany(context.Background(), batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Clear the batch

			// Update progress after successful batch insert
			updateLastProcessedPlaceID(location.PlaceID)
		}

		// Update progress
		progressBar.Increment()
	}

	// Insert remaining batch
	if len(batch) > 0 {
		_, err := collection.InsertMany(context.Background(), batch)
		if err != nil {
			return err
		}
		// Update progress after successful batch insert
		updateLastProcessedPlaceID(batch[len(batch)-1].(Location).PlaceID)
	}

	progressBar.Finish()
	return nil
}

// Helper function to parse float from string
func parseFloat(val string) float64 {
	parsedVal, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0.0
	}
	return parsedVal
}

// Get the last processed PlaceID from file
func getLastProcessedPlaceID() (string, error) {
	data, err := os.ReadFile(progressFile)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil // File doesn't exist, start from the beginning
		}
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// Update the last processed PlaceID to file
func updateLastProcessedPlaceID(placeID string) {
	err := os.WriteFile(progressFile, []byte(placeID), 0644)
	if err != nil {
		log.Printf("Error updating progress file: %v", err)
	}
}

func main() {

	if err := godotenv.Load(".env"); err != nil {
		log.Fatalf("Error loading .env file")
	}

	// Get values from environment variables
	csvFile := os.Getenv("CSV_FILE")
	if csvFile == "" {
		log.Fatalf("CSV_FILE environment variable not set")
	}
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatalf("MONGO_URI environment variable not set")
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		log.Fatalf("DB_NAME environment variable not set")
	}
	collectionName := os.Getenv("COLLECTION_NAME")
	if collectionName == "" {
		log.Fatalf("COLLECTION_NAME environment variable not set")
	}

	progressFile = strings.Split(csvFile, ".")[0] + progressFile

	// Handle interruption signals
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		fmt.Printf("\nInterrupt received, stopping...\n")
		os.Exit(1)
	}()

	err := processCSV(csvFile, mongoURI, dbName, collectionName)
	if err != nil {
		log.Fatalf("Error processing CSV: %v", err)
	}

	fmt.Println("CSV data inserted successfully!")
}
