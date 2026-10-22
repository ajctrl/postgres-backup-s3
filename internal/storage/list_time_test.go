package storage

import (
	"net/http"
	"testing"
)

func TestListingServerTimeUsesFirstPageWithoutLocalFallback(t *testing.T) {
	for _, date := range []string{"Tue, 01 Sep 2026 12:00:00 GMT", "", "not-a-date"} {
		t.Run(date, func(t *testing.T) {
			calls := 0
			s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header()["Date"] = nil // Prevent net/http from manufacturing a Date.
				if date != "" {
					w.Header().Set("Date", date)
				}
				if calls == 1 {
					writeXML(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken></ListBucketResult>`)
				} else {
					w.Header().Set("Date", "Wed, 02 Sep 2026 12:00:00 GMT")
					writeXML(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				}
			})
			listing, err := s.List(t.Context(), "backup/")
			if err != nil || calls != 2 {
				t.Fatalf("list: %v calls=%d", err, calls)
			}
			want, _ := http.ParseTime(date)
			if !listing.ServerTime.Equal(want) {
				t.Fatalf("time=%s want=%s", listing.ServerTime, want)
			}
		})
	}
}
