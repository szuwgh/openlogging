package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// cmd/install.go
func init() {
	rootCmd.AddCommand(installCmd)
	//installCmd.Flags().StringP("install", "I", "all", "install software")
}

// rootCmd represents the base command when called without any subcommands
var installCmd = &cobra.Command{
	Use:   "install",
	Short: "install software",
	Long:  `install a lot of software`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		install(args)
	},
}

func install(args []string) {
	var cmd string
	if len(args) == 0 {
		cmd = "all"
	} else {
		cmd = args[0]
	}
	switch cmd {
	case "all":
		fmt.Println("install all")
	case "a":
		fmt.Println("install a")
	}

}
