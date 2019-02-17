package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	var pbind = flag.String("bind", ":8080", "bind")
	flag.Parse()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		r.Body.Close()
		w.Write(b)
	})

	log.Printf("binding to %s", *pbind)
	log.Fatal(http.ListenAndServe(*pbind, nil))
}
