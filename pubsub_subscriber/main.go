package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type Receive struct {
	Trx_id        string `json:"trx_id"`
	Receiver_rek  string `json:"receiver_rek"`
	Amount        int    `json:"amount"`
	Nama_pengirim string `json:"nama_pengirim"`
	Rek_pengirim  string `json:"rek_pengirim"`
	Catatan       string `json:"catatan"`
}

var db *gorm.DB

func main() {
	ConnectDB()
	pflag.String("subscription", "example-subscription", "name of subscriber")
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	ctx := context.Background()
	proj := "tenunara-351304"
	client, err := pubsub.NewClient(ctx, proj)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}
	sub := viper.GetString("subscription") // retrieve values from viper instead of pflag
	t := createTopicIfNotExists(client, "my-topic")
	// Create a new subscription.
	if err := create(client, sub, t); err != nil {
		// log.Fatal(err)
		fmt.Println("Subscriber already exists")
	}
	// Pull messages via the subscription.
	err = pullMsgs(client, sub, t)
	if err != nil {
		fmt.Println("Messages can't create")
		// log.Fatal(err)
	}
}
func ConnectDB() {
	var err error
	host := "172.23.0.2"
	p := "5432"
	user := "postgres"
	password := "gunawan"
	dbname := "postgres"

	port, _ := strconv.ParseUint(p, 10, 32)
	// Connection URL to connect to Postgres Database
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	// Connect to the DB and initialize the DB variable
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "trx.",
			SingularTable: true,
		},
	})

	if err != nil {
		panic("failed to connect database")
	}

	fmt.Println("Connection Opened to Database")

	// Migrate the database
	var receive Receive
	db.AutoMigrate(&receive)
	fmt.Println("Database Migrated")
}
func createTopicIfNotExists(c *pubsub.Client, topic string) *pubsub.Topic {
	ctx := context.Background()
	t := c.Topic(topic)
	ok, err := t.Exists(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		return t
	}
	t, err = c.CreateTopic(ctx, topic)
	if err != nil {
		log.Fatalf("Failed to create the topic: %v", err)
	}
	return t
}
func create(client *pubsub.Client, name string, topic *pubsub.Topic) error {
	ctx := context.Background()
	// [START pubsub_create_pull_subscription]
	sub, err := client.CreateSubscription(ctx, name, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second,
	})
	if err != nil {
		return err
	}
	fmt.Printf("Created subscription: %v\n", sub)
	// [END pubsub_create_pull_subscription]
	return nil
}
func pubMsgs(client *pubsub.Client, name string, topic *pubsub.Topic, msgs string) error {
	ctx := context.Background()

	// Publish 10 messages on the topic.
	var results []*pubsub.PublishResult
	res := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(msgs),
	})
	fmt.Println(msgs + "Published! \n")
	results = append(results, res)

	// Check that all messages were published.
	for _, r := range results {
		_, err := r.Get(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func pullMsgs(client *pubsub.Client, name string, topic *pubsub.Topic) error {
	ctx := context.Background()
	var result []string

	var mu sync.Mutex
	received := 0
	sub := client.Subscription(name)
	cctx, cancel := context.WithCancel(ctx)
	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		fmt.Printf("Transaksi masuk, ID : %q\n", string(msg.Data))
		result = append(result, string(msg.Data))
		ProcessTransaction(client, name, topic, string(msg.Data))
		mu.Lock()
		defer mu.Unlock()
		received++
		if received == 10 {
			cancel()
		}
	})
	if err != nil {
		return err
	}
	return nil
}
func ProcessTransaction(client *pubsub.Client, name string, topic *pubsub.Topic, result string) {
	var receive Receive
	httpclient := &http.Client{}
	url := fmt.Sprintf("http://localhost:3000/api/trx/status/" + result)
	body, _ := json.Marshal(map[string]interface{}{
		"status": 1,
	})

	err := db.Raw("SELECT trx.transaction.trx_id, trx.transaction.bank_code, trx.transaction.receiver_rek, trx.transaction.amount, trx.user.user_name, trx.account.account_rek FROM trx.transaction JOIN trx.user ON trx.transaction.user_id = trx.user.user_id JOIN trx.account ON trx.transaction.account_id = trx.account.account_id WHERE trx.transaction.trx_id = ?", result).First(&receive).Error
	if err != nil {
		err = pubMsgs(client, name, topic, result)
		if err != nil {
			fmt.Println("Messages won't publish")
		}
	}
	fmt.Println("[GET] Transaction detail")

	response, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
	if err != nil {
		fmt.Print(err.Error())
	}

	response.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := httpclient.Do(response)
	if err != nil {
		err = pubMsgs(client, name, topic, result)
		if err != nil {
			fmt.Println("Messages won't publish")
		}
	}

	fmt.Println("[SUCCESS] Transaksi berhasil!")
	fmt.Println(resp)

}
