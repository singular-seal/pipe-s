package main

import (
	"fmt"
	"github.com/singular-seal/pipe-s/pkg/task"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

func main() {
	viper.AutomaticEnv()

	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			config := viper.GetString("config")
			if len(config) == 0 {
				fmt.Println("Config parameter missing.")
				os.Exit(1)
			}
			t, err := task.NewTaskFromJson(config)
			if err != nil {
				fmt.Printf("Fail to create task from config - %+v\n", err)
				os.Exit(1)
			}
			err = t.Start()
			if err != nil {
				fmt.Printf("Task has error - %+v\n", err)
			}
		},
	}

	cmd.Flags().StringP("config", "c", "", "config file")
	viper.BindPFlag("config", cmd.Flags().Lookup("config"))

	cmd.Execute()
}
