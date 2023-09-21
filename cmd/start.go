package cmd

import (
	"log"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/szuwgh/hawkobserve/pkg/server"

	"github.com/szuwgh/hawkobserve/pkg/tokenizer"
	_ "github.com/szuwgh/hawkobserve/pkg/tokenizer/buildinit"
	"github.com/szuwgh/hawkobserve/web"
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
		log.Fatalln("hawkobserve handler is nil")
	}
	webHandler.Run()
}
