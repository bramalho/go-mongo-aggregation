package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Podcast represents the schema for the "Podcasts" collection
type Podcast struct {
	ID     primitive.ObjectID `bson:"_id,omitempty"`
	Title  string             `bson:"title,omitempty"`
	Author string             `bson:"author,omitempty"`
	Tags   []string           `bson:"tags,omitempty"`
}

// Episode represents the schema for the "Episodes" collection
type Episode struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Podcast     primitive.ObjectID `bson:"podcast,omitempty"`
	Title       string             `bson:"title,omitempty"`
	Description string             `bson:"description,omitempty"`
	Duration    int32              `bson:"duration,omitempty"`
}

// PodcastEpisode represents an aggregation result-set for two collections
type PodcastEpisode struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Podcast     Podcast            `bson:"podcast,omitempty"`
	Title       string             `bson:"title,omitempty"`
	Description string             `bson:"description,omitempty"`
	Duration    int32              `bson:"duration,omitempty"`
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("DB")))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(ctx)

	database := client.Database("quickstart")
	collection := database.Collection("episodes")

	// addData(database, ctx)

	getData(collection, ctx)
	getDataAggregated(collection, ctx)
	getDataPodcastEpisode(collection, ctx)
}

func addData(database *mongo.Database, ctx context.Context) {
	podcast := Podcast{
		Title:  "My Awesome Podcast",
		Author: "Bruno",
		Tags: []string{
			"test",
			"demo",
		},
	}

	podcastsCollection := database.Collection("podcasts")
	result, err := podcastsCollection.InsertOne(ctx, podcast)
	if err != nil {
		log.Fatal(err)
	}

	podcast.ID = result.InsertedID.(primitive.ObjectID)

	var episodes []interface{} = make([]interface{}, 2)
	episodes[0] = Episode{
		Podcast:     podcast.ID,
		Title:       "Episode One",
		Description: "My Awesome Episode One",
		Duration:    1,
	}
	episodes[1] = Episode{
		Podcast:     podcast.ID,
		Title:       "Episode Two",
		Description: "My Awesome Episode Two",
		Duration:    2,
	}

	episodesCollection := database.Collection("episodes")
	_, err = episodesCollection.InsertMany(ctx, episodes)
	if err != nil {
		log.Fatal(err)
	}
}

func getData(collection *mongo.Collection, ctx context.Context) {
	var episodes []Episode

	cursor, err := collection.Find(ctx, Episode{})
	if err != nil {
		log.Fatal(err)
	}

	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var episode Episode
		cursor.Decode(&episode)
		episodes = append(episodes, episode)
	}

	fmt.Printf("\n\n\033[1;31m%s\033[0m\n", "Get Data")
	spew.Dump(episodes)
}

func getDataAggregated(collection *mongo.Collection, ctx context.Context) {
	lookupStage := bson.D{{"$lookup", bson.D{{"from", "podcasts"}, {"localField", "podcast"}, {"foreignField", "_id"}, {"as", "podcast"}}}}
	unwindStage := bson.D{{"$unwind", bson.D{{"path", "$podcast"}, {"preserveNullAndEmptyArrays", false}}}}

	showLoadedCursor, err := collection.Aggregate(ctx, mongo.Pipeline{lookupStage, unwindStage})
	if err != nil {
		panic(err)
	}

	var episodes []bson.M
	if err = showLoadedCursor.All(ctx, &episodes); err != nil {
		panic(err)
	}

	fmt.Printf("\n\n\033[1;33m%s\033[0m\n", "Get Data Aggregated")
	spew.Dump(episodes)
}

func getDataPodcastEpisode(collection *mongo.Collection, ctx context.Context) {
	lookupStage := bson.D{{"$lookup", bson.D{{"from", "podcasts"}, {"localField", "podcast"}, {"foreignField", "_id"}, {"as", "podcast"}}}}
	unwindStage := bson.D{{"$unwind", bson.D{{"path", "$podcast"}, {"preserveNullAndEmptyArrays", false}}}}

	showLoadedStructCursor, err := collection.Aggregate(ctx, mongo.Pipeline{lookupStage, unwindStage})
	if err != nil {
		panic(err)
	}

	var episodes []PodcastEpisode
	if err = showLoadedStructCursor.All(ctx, &episodes); err != nil {
		panic(err)
	}

	fmt.Printf("\n\n\033[1;32m%s\033[0m\n", "Get Data PodcastEpisode")
	spew.Dump(episodes)
}
