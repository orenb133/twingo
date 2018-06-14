package twingo 

import (
	"os"
	"github.com/kardianos/osext"
	"github.com/op/go-logging"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

//=====================================================================================================================
type Configuration struct {
	viper *viper.Viper
	flags *pflag.FlagSet
	logger *logging.Logger
	executableDirectoryPath string
	configFilePath string
}

//---------------------------------------------------------------------------------------------------------------------
func NewConfiguration(aLogger *logging.Logger) *Configuration {
	instance := new(Configuration)
	instance.viper = viper.New()
	instance.flags = pflag.NewFlagSet("twingo", pflag.ExitOnError)
	instance.logger = aLogger
	var err error
	instance.executableDirectoryPath, err = osext.ExecutableFolder()
	if err != nil {
		aLogger.Fatalf("Failed getting executable directory path: %s", err)
	}
	
	instance.registerFlags()

	return instance	
}

//---------------------------------------------------------------------------------------------------------------------
func (this *Configuration) LoadOrExit() {
	this.flags.Parse(pflag.CommandLine.Args())
	this.viper.SetConfigFile(this.configFilePath)
	err := this.viper.ReadInConfig()
	
	if err != nil {
		this.logger.Errorf("Failed loading configuration file: %s", err)
		os.Exit(2)		
	}
	
	this.logger.Noticef("%v", this.viper.GetStringSlice("logger.level"))
}

//---------------------------------------------------------------------------------------------------------------------
func (this *Configuration) registerFlags() {
	this.flags.StringVarP(&this.configFilePath, "config-file-path", "c", 
						  this.executableDirectoryPath + string(os.PathSeparator) + "config.toml", 
						  "Path to configuration file")
}

