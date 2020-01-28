package main

import (
	"log"
	"os"
	"strings"
	"time"
	"strconv"
	"encoding/json"
    "github.com/dghubble/go-twitter/twitter"
    "github.com/agriuseatstweets/go-pubbers/pubbers"
	"github.com/confluentinc/confluent-kafka-go-dev/kafka"
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
	return []byte(t.Time.Format(time.RubyDate)), nil
}

type UBOriginal struct {
	ID int64 `json:"id,omitempty"`
	Userid int64 `json:"userID,omitempty"`
	Text string `json:"text,omitempty"`
    Source string `json:"source,omitempty"`
    Idate Time `json:"iDate,omitempty"`
    Truncated int `json:"truncated,omitempty"`
    Retweetid int64 `json:"retweetID,omitempty"`
    Retweetuserid int64 `json:"retweetUserID,omitempty"`
    Retweet int `json:"retweet,omitempty"`
    Sensitive int `json:"sensitive,omitempty"`
    Lang string `json:"lang,omitempty"`
    Created Time `json:"created,omitempty"`
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
	twitter.Tweet
	TH_rehydrated bool `json:"th_rehydrated"`
	TH_original UBOriginal `json:"th_original"`
	msg kafka.Message
}

type UnRehydratedTweet struct {
	ID int64 `json:"id"`
	CreatedAt Time `json:"created_at"`
	TH_rehydrated bool `json:"th_rehydrated"`
	TH_original UBOriginal `json:"th_original"`
	msg kafka.Message
}

type AgriusTweet interface {
	getTweetIDStr() string
}

func (t RehydratedTweet) getTweetIDStr() string {
	return strconv.FormatInt(t.ID, 10)
}

func (t UnRehydratedTweet) getTweetIDStr() string {
	return strconv.FormatInt(t.ID, 10)
}

func format(original UBOriginal, tweet twitter.Tweet) AgriusTweet {
	rehydrated := tweet.Text != ""

	if rehydrated == false {
		return UnRehydratedTweet{
			ID: original.ID,
			CreatedAt: original.Created,
			TH_rehydrated: rehydrated,
			TH_original: original,
			msg: original.msg,
		}
	}

	// TODO: make omitempty do what you want in Tweet

	return RehydratedTweet{
			Tweet: tweet,
			TH_rehydrated: rehydrated,
			TH_original: original,
			msg: original.msg,
	}
}

func getIds (ogs []UBOriginal) []int64{
	var ids []int64
	for _, t := range ogs {
		ids = append(ids, t.ID)
	}
	return ids
}


func waitLookup(client *twitter.Client, v []UBOriginal, errs chan error) []twitter.Tweet {
	ents := true
	mp := true

	params := twitter.StatusLookupParams{
		IncludeEntities: &ents,
		Map: &mp,
	}

	ids := getIds(v)

	for {
		tweets, httpResponse, err := client.Statuses.Lookup(ids, &params)

		if err == nil && len(tweets) > 0 {
			return tweets
		}
		HandleErrors(err, httpResponse, errs)
	}
}


func rehydrate(client *twitter.Client, inchan chan []UBOriginal, errs chan error) (chan AgriusTweet) {
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


func prepTweets(tweets chan AgriusTweet, errs chan error) chan pubbers.QueuedMessage {
	out := make(chan pubbers.QueuedMessage)
	go func(){
		defer close(out)
		for tw := range tweets {
			id := []byte(tw.getTweetIDStr())
			t, err := json.Marshal(tw)

			if err != nil {
				errs <- err
				continue
			}
			out <- pubbers.QueuedMessage{id, t}
		}
	}()
	return out
}


func monitor(errs <-chan error) {
	e := <- errs
	log.Fatalf("Rehydrate failed with error: %v", e)
}


func main() {
	client := getTwitterClient()

	errs := make(chan error)
	go monitor(errs)

	p, err := pubbers.NewKafkaWriter()
	if err != nil {
		errs <- err
	}

	size, _ := strconv.Atoi(os.Getenv("REHYDRATE_SIZE"))
 	consumer := NewKafkaConsumer()
	ogs := consumer.Consume(size, 100, errs)

	rts := rehydrate(client, ogs, errs)
	messages := prepTweets(rts, errs)
	results := p.Publish(messages, errs)

	_, _ = consumer.Consumer.Commit()

	log.Printf("Publisher closed, published %v tweets out of %v sent", results.Written, results.Sent)
}
