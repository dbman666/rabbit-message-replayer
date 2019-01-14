package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/coveo/gotemplate/collections"
	"github.com/coveo/gotemplate/errors"
	"github.com/coveo/gotemplate/hcl"
	"github.com/coveo/gotemplate/json"
	"github.com/coveo/gotemplate/utils"
	"github.com/coveo/gotemplate/yaml"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/streadway/amqp"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Version is initialized at build time through -ldflags "-X main.Version=<version number>"
var version = "1.0.0"

const (
	rabbitUser     = "RABBIT_USER"
	rabbitPassword = "RABBIT_PASSWORD"
	rabbitHost     = "RABBIT_HOST"
)

const description = `
A tool to reinject orphaned messages into RabbitMQ following a persistent_store or queues index corruption.
`

var (
	print     = utils.ColorPrint
	printf    = utils.ColorPrintf
	println   = utils.ColorPrintln
	errPrintf = utils.ColorErrorPrintf
	must      = errors.Must
	iif       = collections.IIf
)

func main() {
	var exitCode int

	defer func() {
		if rec := recover(); rec != nil {
			errPrintf(color.RedString("Recovered %v\n"), rec)
			debug.PrintStack()
			exitCode = -1
		}
		os.Exit(exitCode)
	}()

	var (
		app              = kingpin.New(os.Args[0], description)
		forceColor       = app.Flag("color", "Force rendering of colors event if output is redirected.").Bool()
		forceNoColor     = app.Flag("no-color", "Force rendering of colors event if output is redirected.").Bool()
		verbose          = app.Flag("verbose", "Indicate to add traces during processing").Short('V').Bool()
		getVersion       = app.Flag("version", "Get the current version of gotemplate.").Short('v').Bool()
		rabbitURL        = app.Flag("rabbit-host", "The RabbitMQ Url. Env="+rabbitHost).Short('H').Envar(rabbitHost).String()
		rabbitPrototocol = app.Flag("protocol", "The RabbitMQ protocol.").Default("amqp").String()
		rabbitPort       = app.Flag("port", "The RabbitMQ port.").Default("5672").Int()
		user             = app.Flag("user", "User used to connect to RabbitMQ. Env="+rabbitUser).Short('u').Default("guest").Envar(rabbitUser).String()
		password         = app.Flag("password", "Password used to connect to RabbitMQ. Env="+rabbitPassword).Default("guest").Envar(rabbitPassword).String()
		replay           = app.Flag("replay", "Actually replay the messages to the target Rabbit cluster.").Short('r').Bool()
		folder           = app.Flag("folder", "Folder where to find messages.").Short('f').Default(".").String()
		maxDepth         = app.Flag("max-depth", "Maximum depth to find.").Default("5").Int()
		patterns         = app.Flag("pattern", "Pattern used to find persistent store or index files.").Short('p').Default("*.rdq", "*.idx").Strings()
		threads          = app.Flag("threads", "Number of parallel threads running.").Short('t').Default(fmt.Sprint((runtime.NumCPU() + 1) / 2)).Int()
		output           = app.Flag("output", "Specify the output type (Json, Yaml, Hcl)").Short('o').Enum("Hcl", "h", "hcl", "H", "HCL", "Json", "j", "json", "J", "JSON", "Yaml", "Yml", "y", "yml", "yaml", "Y", "YML", "YAML")
		declareQueue     = app.Flag("declare-queues", "Force queue creation if it does not exist").Bool()
		match            = app.Flag("match", "Regular expression for matching queues").Short('m').PlaceHolder("regexp").String()
	)

	app.UsageWriter(os.Stdout)
	kingpin.CommandLine = app
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()

	var patternList []string
	for _, p := range *patterns {
		patternList = append(patternList, strings.Split(p, ";")...)
	}

	if *threads == 0 {
		*threads = runtime.NumCPU() / 2
	}
	if *forceNoColor {
		color.NoColor = true
	} else if *forceColor {
		color.NoColor = false
	}

	if *getVersion {
		println(version)
		os.Exit(0)
	}

	var re *regexp.Regexp
	if *match != "" {
		re = regexp.MustCompile(*match)
	}

	files := utils.MustFindFilesMaxDepth(*folder, *maxDepth, false, patternList...)
	files = collections.AsList(files).Unique().Strings()
	if *verbose {
		errPrintf(color.GreenString("%d %s on %d thread(s)\n", len(files), "file(s) to process", *threads))
	}

	url := fmt.Sprintf("%s://%s:%s@%s:%d", *rabbitPrototocol, *user, *password, *rabbitURL, *rabbitPort)

	// Start multithreads processing
	jobs := make(chan string, *threads)
	results := make(chan RabbitFile, len(files))
	var publish chan *RabbitMessage
	if *replay {
		publish = make(chan *RabbitMessage, *threads*30)
	}
	for i := 0; i < *threads; i++ {
		go fileHandler(i, jobs, results, re)

		if *replay {
			go messageHandler(i, url, publish, *declareQueue)
		}
	}

	// Add the files to process
	for _, file := range files {
		jobs <- file
	}
	close(jobs)

	// Wait for results
	var queueStat, qtStat, fileStat, ftStat Statistics
	for range files {
		file := <-results
		if *verbose {
			errPrintf("%s %d messages %d bytes\n", file.Name(), file.Count(), file.Size())
		}

		queueStat.Join(file.Queues)
		if file.Count() > 0 {
			fileStat.AddStatistic(file.Stat)
			ftStat.AddGroup(file.Type(), file.Stat)
		}

		if *replay {
			for _, msg := range file.Messages {
				publish <- msg
			}
		}
	}
	for _, qs := range queueStat.List {
		qtStat.AddGroup(strings.TrimPrefix(filepath.Ext(qs.Name), "."), *qs)
	}

	if mode := *output; mode != "" {
		switch strings.ToUpper(mode[:1]) {
		case "Y":
			collections.ListHelper = yaml.GenericListHelper
			collections.DictionaryHelper = yaml.DictionaryHelper
		case "H":
			collections.ListHelper = hcl.GenericListHelper
			collections.DictionaryHelper = hcl.DictionaryHelper
		default:
			collections.ListHelper = json.GenericListHelper
			collections.DictionaryHelper = json.DictionaryHelper
		}
		print(collections.AsList(map[string]interface{}{
			"Files":      fileStat.GetStats(),
			"FileTypes":  ftStat.GetStats(),
			"Queues":     queueStat.GetStats(),
			"QueueTypes": qtStat.GetStats(),
		}).PrettyPrint())
	} else {
		printTable := func(title string, listStat Statistics, group bool) {
			columns := collections.NewList(title, "Count", "Messages", "Size", "Average", "Minimum", "Maximum")
			if !group {
				columns = columns.Remove(1)
			}

			table := tablewriter.NewWriter(os.Stdout)
			table.SetBorder(false)
			table.SetRowSeparator("-")
			table.SetColumnSeparator(" ")
			table.SetCenterSeparator(" ")
			table.SetHeader(columns.Strings())
			table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)

			var stat Statistic
			for _, s := range listStat.List {
				data := collections.NewList(s.Name, s.Count(), s.Messages(), int64(s.Sum()), int64(s.Average()), int64(s.Minimum()), int64(s.Maximum()))
				if !group {
					data = data.Remove(1)
				}
				table.Append(data.Strings())
				stat.Join(*s)
			}
			if len(listStat.List) > 1 {
				s := stat
				data := collections.NewList(len(listStat.List), s.Count(), s.Messages(), int64(s.Sum()), int64(s.Average()), int64(s.Minimum()), int64(s.Maximum()))
				if !group {
					data = data.Remove(1)
				}
				table.SetFooter(data.Strings())
			}
			table.Render()
			fmt.Println()
		}

		printTable("Files", fileStat, false)
		printTable("Queues", queueStat, false)
		printTable("Queue Types", qtStat, true)
		printTable("File Types", ftStat, true)
	}
}

func fileHandler(id int, jobs <-chan string, result chan<- RabbitFile, reMatch *regexp.Regexp) {
	for file := range jobs {
		data, err := ReadRabbitFile(file, reMatch)
		if err != nil {
			errPrintf("Unable to read %s", file)
		}
		data.ProcessMessages(nil)
		result <- data
	}
}

func messageHandler(id int, url string, messages <-chan *RabbitMessage, declareQueues bool) {
	conn := must(amqp.Dial(url)).(*amqp.Connection)
	defer conn.Close()

	ch := must(conn.Channel()).(*amqp.Channel)
	defer ch.Close()

	go func() {
		returned := ch.NotifyReturn(make(chan amqp.Return, 1))
		for r := range returned {
			fmt.Println("Returned message", r.RoutingKey)
		}
	}()

	for msg := range messages {
		if declareQueues {
			must(ch.QueueDeclare(msg.Queue, true, false, false, false, nil))
		}

		pub := amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         msg.Data,
		}

		if msg.IsPush() {
			pub.Headers = map[string]interface{}{
				"cmf": fmt.Sprintf("{url:%s,method:Process,zip:true}", msg.Queue),
			}
		}

		must(ch.Publish("", msg.Queue, true, false, pub))
	}
}
