package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/couchbase/cbauth"
	c "github.com/couchbase/indexing/secondary/common"
	"github.com/couchbase/indexing/secondary/dataport"
	"github.com/couchbase/indexing/secondary/logging"
	"github.com/couchbase/indexing/secondary/platform"
	"github.com/couchbase/indexing/secondary/projector"
)

var done = make(chan bool)

var options struct {
	adminport   string
	numVbuckets int
	kvaddrs     string
	logFile     string
	auth        string
	loglevel    string
	diagDir     string
	isIPv6      bool
}

func argParse() string {
	fset := flag.NewFlagSet("projector", flag.ContinueOnError)
	fset.StringVar(&options.adminport, "adminport", "", "adminport address")
	fset.IntVar(&options.numVbuckets, "vbuckets", 1024, "maximum number of vbuckets configured.")
	// kvaddrs is passed in from ns-server.  For ipv6, it is expected that ns-server will pass in a proper address.
	fset.StringVar(&options.kvaddrs, "kvaddrs", "127.0.0.1:12000", "comma separated list of kvaddrs")
	fset.StringVar(&options.logFile, "logFile", "", "output logs to file default is stdout")
	fset.StringVar(&options.loglevel, "logLevel", "Info", "Log Level - Silent, Fatal, Error, Info, Debug, Trace")
	fset.StringVar(&options.auth, "auth", "", "Auth user and password")
	fset.StringVar(&options.diagDir, "diagDir", "./", "Directory for writing projector diagnostic information")
	fset.BoolVar(&options.isIPv6, "ipv6", false, "IPV6 cluster")

	logging.Infof("Parsing the args")

	for i := 1; i < len(os.Args); i++ {
		if err := fset.Parse(os.Args[i : i+1]); err != nil {
			if strings.Contains(err.Error(), "flag provided but not defined") {
				logging.Warnf("Ignoring the unspecified argument error: %v", err)
			} else {
				c.CrashOnError(err)
			}
		}
	}

	args := fset.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}
	return args[0]
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <cluster-addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	platform.HideConsole(true)
	defer platform.HideConsole(false)
	c.SeedProcess()

	logging.Infof("Projector started with command line: %v\n", os.Args)

	cluster := argParse() // eg. "localhost:9000"

	config := c.SystemConfig.Clone()
	logging.SetLogLevel(logging.Level(options.loglevel))

	config.SetValue("maxVbuckets", options.numVbuckets)
	if f := getlogFile(); f != nil {
		fmt.Printf("Projector logging to %q\n", f.Name())
		logging.SetLogWriter(f)
		config.SetValue("log.file", f.Name())
	}
	config.SetValue("projector.clusterAddr", cluster)
	config.SetValue("projector.adminport.listenAddr", options.adminport)
	config.SetValue("projector.diagnostics_dir", options.diagDir)

	if err := os.MkdirAll(options.diagDir, 0755); err != nil {
		c.CrashOnError(err)
	}

	// setup cbauth
	if options.auth != "" {
		up := strings.Split(options.auth, ":")
		if _, err := cbauth.InternalRetryDefaultInit(cluster, up[0], up[1]); err != nil {
			logging.Fatalf("Failed to initialize cbauth: %s", err)
		}
	}

	epfactory := NewEndpointFactory(cluster, options.numVbuckets)
	config.SetValue("projector.routerEndpointFactory", epfactory)

	logging.Infof("%v\n", c.LogOs())
	logging.Infof("%v\n", c.LogRuntime())

	c.SetIpv6(options.isIPv6)

	go c.ExitOnStdinClose()
	projector.NewProjector(options.numVbuckets, config)

	<-done
}

// NewEndpointFactory to create endpoint instances based on config.
func NewEndpointFactory(cluster string, nvbs int) c.RouterEndpointFactory {

	return func(topic, endpointType, addr string, config c.Config) (c.RouterEndpoint, error) {
		switch endpointType {
		case "dataport":
			return dataport.NewRouterEndpoint(cluster, topic, addr, nvbs, config)
		default:
			logging.Fatalf("Unknown endpoint type\n")
		}
		return nil, nil
	}
}

func getlogFile() *os.File {
	switch options.logFile {
	case "":
		return nil
	case "tempfile":
		f, err := ioutil.TempFile("", "projector")
		if err != nil {
			logging.Fatalf("%v", err)
		}
		return f
	}
	f, err := os.Create(options.logFile)
	if err != nil {
		logging.Fatalf("%v", err)
	}
	return f
}
