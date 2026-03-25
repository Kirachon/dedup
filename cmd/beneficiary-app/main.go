package main

import (
	"context"
	"log"

	appcore "dedup/internal/app"
	"dedup/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if err := appcore.Run(context.Background(), cfg); err != nil {
		log.Fatalf("run app: %v", err)
	}
}
