package cmd

import (
	"fmt"
	"log"
	"runtime"

	"github.com/sophon-lab/temsearch/pkg/tokenizer"
	_ "github.com/sophon-lab/temsearch/pkg/tokenizer/buildinit"
	"github.com/sophon-lab/temsearch/web"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(StartCmd)
}

// rootCmd represents the base command when called without any subcommands
var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "start software",
	Long:  `start a lot of software`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("start")
		start(args)
	},
}

func start(args []string) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	tokenizer.Init()
	webHandler := web.New()
	webHandler.Run()
}
