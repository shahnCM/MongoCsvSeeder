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

type Location struct {
	Type        string     `json:"type"`
	Coordinates [2]float64 `json:"coordinates"`
}

type Place struct {
	PlaceID               string     `json:"placeId" bson:"placeId"`
	Address               string     `json:"address" bson:"address"`
	Version               string     `json:"version" bson:"version"`
	IsAutoCompleteAddress bool       `json:"isAutoCompleteAddress" bson:"isAutoCompleteAddress"`
	Types                 []string   `json:"types" bson:"types"`
	PlusCode              string     `json:"plusCode" bson:"plusCode"`
	City                  string     `json:"city" bson:"city"`
	Division              string     `json:"division" bson:"division"`
	District              string     `json:"district" bson:"district"`
	PostalCode            string     `json:"postalCode" bson:"postalCode"`
	Sublocality           string     `json:"sublocality" bson:"sublocality"`
	LocalArea             string     `json:"localArea" bson:"localArea"`
	Location              *Location  `json:"location" bson:"location"`
	Suggestions           []any      `json:"suggestions" bson:"suggestions"`
	Reviews               []any      `json:"reviews" bson:"reviews"`
	MergedAt              *time.Time `json:"mergedAt" bson:"mergedAt"`
	IsMerged              bool       `json:"isMerged" bson:"isMerged"`
}

// File to store the last processed PlaceID
var progressFile = "_progress.txt"

func parseArrayFromColumn(csv string) []string {
	// Remove the square brackets and spaces
	cleaned := strings.Trim(csv, "[]")

	// Split the string by commas
	items := strings.Split(cleaned, ",")

	// Trim any surrounding whitespace or quotes from each element
	for i, item := range items {
		items[i] = strings.TrimSpace(strings.Trim(item, "'"))
	}

	return items
}

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

	// Read the header
	header, err := reader.Read()
	if err != nil {
		fmt.Println("Error reading header:", err)
		return err
	}
	fmt.Println("Header:", header)

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
		// if record[4] != "Bangladesh" {
		// 	progressBar.Increment()
		// 	continue
		// }

		place := Place{
			PlaceID:               record[0],
			Address:               record[2],
			Version:               record[12],
			IsAutoCompleteAddress: strings.ToLower(record[3]) == "true",
			Types:                 parseArrayFromColumn(record[1]),
			PlusCode:              record[8],
			City:                  record[5],
			Division:              record[6],
			District:              record[7],
			PostalCode:            record[11],
			Sublocality:           record[13],
			LocalArea:             record[14],
			Location: &Location{
				Type:        "Point",
				Coordinates: [2]float64{parseFloat(record[10]), parseFloat(record[9])},
			},

			Suggestions: []any{},
			Reviews:     []any{},
			MergedAt:    nil,
			IsMerged:    false,
		}

		batch = append(batch, place)

		if len(batch) >= batchSize {
			_, err := collection.InsertMany(context.Background(), batch)
			if err != nil {
				return err
			}
			batch = batch[:0] // Clear the batch

			// Update progress after successful batch insert
			updateLastProcessedPlaceID(place.PlaceID)
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
		updateLastProcessedPlaceID(batch[len(batch)-1].(Place).PlaceID)
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
