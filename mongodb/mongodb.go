package mongodb

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var mongoClient *mongo.Client

func InitMongodb(uri string, poolSize uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts := options.Client().ApplyURI(uri)
	if poolSize > 0 {
		opts.SetMaxPoolSize(poolSize)
	} else {
		opts.SetMaxPoolSize(1024)
	}
	//opts.SetReadPreference(readpref.Primary()) //默认值
	// opts.SetAuth(options.Credential{Username: "root", Password: "password"})
	// opts.SetAuth(options.Credential{AuthSource: "admin", Username: "root", Password: "password"})
	// opts.SetDirect(true)

	var err error
	mongoClient, err = mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatalf("mongodb connect error: %s\n", err)
	}
	// Check the connection
	if err := mongoClient.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatalf("Ping mongodb error: %s\n", err)
	}
}

// connect 获取 db->collection的连接
func connect(db, collection string) *mongo.Collection {
	return mongoClient.Database(db).Collection(collection)
}

func Count(db, collection string, query interface{}) (int64, error) {
	c := connect(db, collection)
	return c.CountDocuments(context.Background(), query)
}

func Insert(db, collection string, docs ...interface{}) error {
	c := connect(db, collection)
	if _, err := c.InsertMany(context.Background(), docs); err != nil {
		return err
	}
	return nil
}

// 如果有多个满足的数据，只会返回一个
func FindOne(db, collection string, query, selector, result interface{}) error {
	c := connect(db, collection)
	res := c.FindOne(context.Background(), query)
	if res.Err() != nil {
		return res.Err()
	}
	return res.Decode(result)
}

func FindAll(db, collection string, query, selector, result interface{}) error {
	c := connect(db, collection)
	opts := options.Find().SetProjection(selector)
	cur, err := c.Find(context.Background(), query, opts)
	if err != nil {
		return err
	}
	return cur.All(context.Background(), result)
}

func FindAllWithSort(db, collection string, query, selector, result interface{}, sortFields interface{}) error {
	c := connect(db, collection)
	opts := options.Find().SetProjection(selector).SetSort(sortFields)
	cur, err := c.Find(context.Background(), query, opts)
	if err != nil {
		return err
	}
	return cur.All(context.Background(), result)
}

// 如果多个满足条件，实际只会更新一个，不会报错
func Update(db, collection string, selector, update interface{}, opts ...*options.UpdateOptions) error {
	c := connect(db, collection)
	if _, err := c.UpdateOne(context.Background(), selector, update, opts...); err != nil {
		return err
	}
	return nil
}

func Upsert(db, collection string, selector, update interface{}) error {
	//https://www.mongodb.com/docs/drivers/go/current/fundamentals/crud/write-operations/upsert/
	c := connect(db, collection)
	opts := options.Update().SetUpsert(true)
	if _, err := c.UpdateOne(context.Background(), selector, update, opts); err != nil {
		return err
	}
	return nil
}

func UpdateAll(db, collection string, selector, update interface{}) error {
	c := connect(db, collection)
	if _, err := c.UpdateMany(context.Background(), selector, update); err != nil {
		return err
	}
	return nil
}

// 如果有多个数据满足，只会删除一个数据，其他的数据不动
func Remove(db, collection string, selector interface{}) error {
	c := connect(db, collection)
	if _, err := c.DeleteOne(context.Background(), selector); err != nil {
		return err
	}
	return nil
}

func RemoveAll(db, collection string, selector interface{}) error {
	c := connect(db, collection)
	if _, err := c.DeleteMany(context.Background(), selector); err != nil {
		return err
	}
	return nil
}

func PipeOne(db, collection string, pipeline, result interface{}) error {
	c := connect(db, collection)
	cur, err := c.Aggregate(context.Background(), pipeline)
	if err != nil {
		return err
	}
	return cur.All(context.Background(), result)
}

// Index 创建索引
func Index(db, collection string, models []mongo.IndexModel) error {
	c := connect(db, collection)
	_, err := c.Indexes().CreateMany(context.Background(), models)
	return err
}

// DropIndex 删除索引， 删除索引时，key里一定要包含order值，比如创建的索引为：
//
//	Keys: bson.M{"operation_type": 1}
//
// 删除时的key则为：operation_type_1
func DropIndex(db, collection string, key string) error {
	c := connect(db, collection)
	_, err := c.Indexes().DropOne(context.Background(), key)
	return err
}
