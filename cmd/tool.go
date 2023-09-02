package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/charlieegan3/toolbelt/pkg/tool"
	"github.com/spf13/viper"

	pointsServerTool "github.com/charlieegan3/tool-points-server/pkg/tool"
)

func main() {

	viper.SetConfigFile(os.Args[1])
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("failed to read config: %v", err)
	}

	tb := tool.NewBelt()

	toolCfg := viper.Get("tools").(map[string]interface{})
	tb.SetConfig(toolCfg)

	err = tb.AddTool(context.Background(), &pointsServerTool.PointsServer{})
	if err != nil {
		log.Fatalf("failed to add tool: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			cancel()
		}
	}()

	tb.RunServer(ctx, "127.0.0.1", "3000")
}
