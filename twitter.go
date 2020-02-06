package main

import (
	"log"
	"os"
	"fmt"
	"time"
	"strconv"
	"net/http"
    "github.com/dghubble/go-twitter/twitter"
    "github.com/dghubble/oauth1"
)



func ParseRateLimiting(resp *http.Response) (int, time.Duration) {
	remaining, _ := strconv.Atoi(resp.Header["X-Rate-Limit-Remaining"][0])
	reset, _ := strconv.Atoi(resp.Header["X-Rate-Limit-Reset"][0])
	untilReset := reset - int(time.Now().Unix())
	return remaining, time.Duration(untilReset) * time.Second
}

func HandleErrors(err error, httpResponse *http.Response, errs chan error) {
	switch err.(type) {
	case twitter.APIError:

		// could use err.Errors[0].Code, but this seems simpler for now
		switch httpResponse.StatusCode {

		// Twitter rate limits, so sleep until limit resets
		case 429:
			_, reset := ParseRateLimiting(httpResponse)

			// If its short, just wait for it
			if reset.Seconds() < 60 {
				log.Printf("Sleeping %v seconds.", reset)
				time.Sleep(reset)
				return
			}

			// Else, blow up and wait for next cron to run
			err = fmt.Errorf("Rehydrate hit rate limit, which will reset in: %v\n", reset)
			errs <- err
			return

		default:
			errs <- err
			return
		}

	default:
		// HTTP Error from sling. Retry and hope connection improves.
		sleeping := 30 * time.Second
		log.Printf("HTTP Error. Sleeping %v seconds. Error: \n%v\n", sleeping, err)
		time.Sleep(sleeping)
		return
	}
}



func getTwitterClient() *twitter.Client {
	config := oauth1.NewConfig(
		os.Getenv("T_CONSUMER_TOKEN"),
		os.Getenv("T_CONSUMER_SECRET"))

	token := oauth1.NewToken(
		os.Getenv("T_ACCESS_TOKEN"),
		os.Getenv("T_TOKEN_SECRET"))
	httpClient := config.Client(oauth1.NoContext, token)

	return twitter.NewClient(httpClient)
}
