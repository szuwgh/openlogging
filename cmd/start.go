package cmd

import (
	"log"
	"runtime"

	"github.com/sophon-lab/athena/pkg/server"
	"github.com/sophon-lab/athena/pkg/tokenizer"
	_ "github.com/sophon-lab/athena/pkg/tokenizer/buildinit"
	"github.com/sophon-lab/athena/web"
	"github.com/spf13/cobra"
)

var config server.Config

func init() {

	//StartCmd.Flags().StringVarP(&Source, "source", "s", "", "Source directory to read from")
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
		start(args)
	},
}

func start(args []string) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	tokenizer.Init()
	webHandler := web.New()
	if webHandler == nil {
		log.Fatalln("temsearch handler is nil")
	}
	webHandler.Run()
}
