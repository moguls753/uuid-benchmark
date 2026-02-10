package mongodb

import (
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type MongoDBBenchmarker struct {
	client        *mongo.Client
	db            *mongo.Database
	keyType       string
	collName      string
	metricsBefore bson.M // serverStatus snapshot before workload
}

func New() *MongoDBBenchmarker {
	return &MongoDBBenchmarker{}
}
