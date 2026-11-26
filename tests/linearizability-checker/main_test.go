package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func TestFixtures(t *testing.T) {
	files, _ := filepath.Glob("testdata/*.jsonl")
	if len(files) == 0 {
		t.Fatal("no fixtures")
	}
	for _, f := range files {
		name := filepath.Base(f)
		want := porcupine.Ok
		if strings.HasPrefix(name, "illegal-") {
			want = porcupine.Illegal
		}
		t.Run(name, func(t *testing.T) {
			hdr, ops := load(f)
			h, _ := build(hdr, ops)
			got := porcupine.CheckOperationsTimeout(configModel(len(hdr.Types), hdr.Types, initialState(hdr)), h, time.Minute)
			if got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
		})
	}
}
