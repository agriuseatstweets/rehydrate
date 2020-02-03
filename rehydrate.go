package main

import (
	"log"
	"os"
	"sync"
	"fmt"
	"strings"
	"time"
	"strconv"
	"encoding/json"
    "github.com/dghubble/go-twitter/twitter"
    "github.com/agriuseatstweets/go-pubbers/pubbers"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Time struct {
    time.Time
}

func (t *Time) UnmarshalJSON(b []byte) error {
	format := "2006-01-02T15:04:05"
	s := string(b)
	s = strings.Trim(s, `"`)
	parsed, err := time.Parse(format, s)
	if err == nil {
		*t = Time{parsed}
	}
	return err
}

func (t *Time) MarshalJSON() ([]byte, error) {
	s := t.Time.Format(time.RubyDate)
	s = "\"" + s + "\""
	return []byte(s), nil
}

type UBOriginal struct {
	ID int64 `json:"id,omitempty"`
	Userid int64 `json:"userID,omitempty"`
	Text string `json:"text,omitempty"`
    Source string `json:"source,omitempty"`
    Idate *Time `json:"iDate,omitempty"`
    Truncated int `json:"truncated,omitempty"`
    Retweetid int64 `json:"retweetID,omitempty"`
    Retweetuserid int64 `json:"retweetUserID,omitempty"`
    Retweet int `json:"retweet,omitempty"`
    Sensitive int `json:"sensitive,omitempty"`
    Lang string `json:"lang,omitempty"`
    Created *Time `json:"created,omitempty"`
    Lat float64 `json:"lat,omitempty"`
    Lng float64 `json:"lng,omitempty"`
    Placeid string `json:"placeID,omitempty"`
    Placetype string `json:"placeType,omitempty"`
    Placename string `json:"placeName,omitempty"`
    Hashtags string `json:"hashtags,omitempty"`
    Mentions string `json:"mentions,omitempty"`
    Urls string `json:"urls,omitempty"`
    Symbols string `json:"symbols,omitempty"`
    Ufollowers int `json:"ufollowers,omitempty"`
    Ufriends int `json:"ufriends,omitempty"`
    Ulisted int `json:"ulisted,omitempty"`
    Ufavourites int `json:"ufavourites,omitempty"`
    Utweets int `json:"utweets,omitempty"`
    Udefaultprofile int `json:"udefaultProfile,omitempty"`
    Udefaultprofileimage int `json:"udefaultProfileImage,omitempty"`
    Placecountry string `json:"placeCountry,omitempty"`
    Contributoruserid int64 `json:"ContributorUserID,omitempty"`
    Isquotestatus int `json:"isQuoteStatus,omitempty"`
    Numtimesretweeted int `json:"numTimesRetweeted,omitempty"`
    Numlikes int `json:"numLikes,omitempty"`
    Favorite int `json:"favorite,omitempty"`
    Filter string `json:"filter,omitempty"`
    Month int `json:"month,omitempty"`
	msg kafka.Message
}


type RehydratedTweet struct {
	*twitter.Tweet
	TH_rehydrated bool `json:"th_rehydrated"`
	TH_original *UBOriginal `json:"th_original"`
}

type UnRehydratedTweet struct {
	ID int64 `json:"id"`
	CreatedAt *Time `json:"created_at"`
	TH_rehydrated bool `json:"th_rehydrated"`
	TH_original *UBOriginal `json:"th_original"`
}

type AgriusTweet interface {
	getCreatedAt() time.Time
}

func (t RehydratedTweet) getCreatedAt() time.Time {
	created, err := t.CreatedAtTime()

	if err != nil {
		panic(err)
	}

	return created
}

func (t UnRehydratedTweet) getCreatedAt() time.Time {
	return t.CreatedAt.Time
}

func format(original UBOriginal, tweet twitter.Tweet) AgriusTweet {
	rehydrated := tweet.Text != ""

	if rehydrated == false {
		return UnRehydratedTweet{
			ID: original.ID,
			CreatedAt: original.Created,
			TH_rehydrated: rehydrated,
			TH_original: &original,
		}
	}

	return RehydratedTweet{
			Tweet: &tweet,
			TH_rehydrated: rehydrated,
			TH_original: &original,
	}
}

func getIds (ogs []UBOriginal) []int64{
	ids := make([]int64, len(ogs))
	for i, t := range ogs {
		ids[i] = t.ID
	}
	return ids
}

func waitLookup(client *twitter.Client, ogs []UBOriginal, errs chan error) []twitter.Tweet {
	ents := true
	mp := true

	params := twitter.StatusLookupParams{
		IncludeEntities: &ents,
		Map: &mp,
	}

	ids := getIds(ogs)

	for {
		tweets, httpResponse, err := client.Statuses.Lookup(ids, &params)

		// temp to debug crazy exception that was happening...
		if len(tweets) != len(ogs) {
			err = fmt.Errorf("tweets is not the same length as originals: %v vs. %v.\nids is length: %v.\nIDS: %v.", len(tweets), len(ogs), len(ids), ids)

			panic(err)

		}

		if err == nil && len(tweets) > 0 {
			return tweets
		}

		HandleErrors(err, httpResponse, errs)
	}
}

func rehydrate(client *twitter.Client, inchan chan []UBOriginal, errs chan error) (<-chan AgriusTweet) {
	ch := make(chan AgriusTweet)
	go func() {
		defer close(ch)
		for originals := range inchan {
			tweets := waitLookup(client, originals, errs)

			for i, tw := range tweets {
				og := originals[i]
				rt := format(og, tw)
				ch <- rt
			}
		}
	}()
	return ch
}


func prepTweets(tweets <-chan AgriusTweet, errs chan error) chan pubbers.QueuedMessage {
	out := make(chan pubbers.QueuedMessage)
	go func(){
		defer close(out)
		for tw := range tweets {
			created := tw.getCreatedAt()
			key := []byte(created.Format("2006-01-02"))
			t, err := json.Marshal(&tw)

			if err != nil {
				errs <- err
				continue
			}
			out <- pubbers.QueuedMessage{key, t}
		}
	}()
	return out
}

func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("Rehydrate failed with error: %v", e)
}


func merge(cs ...<-chan AgriusTweet) <-chan AgriusTweet {
    var wg sync.WaitGroup
    out := make(chan AgriusTweet)

    output := func(c <-chan AgriusTweet) {
        for n := range c {
            out <- n
        }
        wg.Done()
    }
    wg.Add(len(cs))
    for _, c := range cs {
        go output(c)
    }

    go func() {
        wg.Wait()
        close(out)
    }()
    return out
}

func parallelRehydrate(client *twitter.Client, inchan chan []UBOriginal, n int, errs chan error) (<-chan AgriusTweet) {

	rts := make([]<-chan AgriusTweet, n)
	for i, _ := range rts {
		rts[i] = rehydrate(client, inchan, errs)
	}
	return merge(rts...)
}

func RunBatch(client *twitter.Client, consumer KafkaConsumer, batchSize int, errs chan error) pubbers.WriteResults {

	writer, err := pubbers.NewKafkaWriter()
	if err != nil {
		errs <- err
	}

	ogs := consumer.Consume(batchSize, 100, errs)

	parallel := batchSize / 100

	rts := parallelRehydrate(client, ogs, parallel, errs)
	messages := prepTweets(rts, errs)
	results := writer.Publish(messages, errs)
	tp, err := consumer.Consumer.Commit()

	if err != nil {
		errs <- err
	}

	log.Printf("Committed topic partition: %v", tp)

	return results
}


func main() {
	client := getTwitterClient()

	errs := make(chan error)
	go monitor(errs)

	size, _ := strconv.Atoi(os.Getenv("REHYDRATE_SIZE"))
 	consumer := NewKafkaConsumer()

	batchSize, _ := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	n := size/batchSize
	results := make([]pubbers.WriteResults, n)

	for i, _ := range results {
		results[i] = RunBatch(client, consumer, batchSize, errs)
	}

	written := 0
	sent := 0
	for _, r := range results {
		written += r.Written
		sent += r.Sent
	}

	log.Printf("Publisher closed, published %v tweets out of %v sent and %v receieved", written, sent, size)
}
