package main

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
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

type LostMessagesData struct {
	toFind      int
	found       int
	filePath    string
	fileHandler *os.File
}

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
		app    = kingpin.New(os.Args[0], description)
		folder = app.Flag("folder", "Folder where to find messages.").Short('f').ExistingDir()

		findLostCommand = app.Command("find-lost", "Finds lost messages given a list of queues and how many messages they have lost")
		lostMessages    = findLostCommand.Flag("lost-messages", "Map of lost messages by queue").Required().ExistingFile()
		outputFolder    = findLostCommand.Flag("output-folder", "Where queue data should be exported").Required().String()

		fullCommand      = app.Command("full", "stuff")
		forceColor       = fullCommand.Flag("color", "Force rendering of colors event if output is redirected.").Bool()
		forceNoColor     = fullCommand.Flag("no-color", "Force rendering of colors event if output is redirected.").Bool()
		verbose          = fullCommand.Flag("verbose", "Indicate to add traces during processing").Short('V').Bool()
		getVersion       = fullCommand.Flag("version", "Get the current version of gotemplate.").Short('v').Bool()
		rabbitURL        = fullCommand.Flag("rabbit-host", "The RabbitMQ Url. Env="+rabbitHost).Short('H').Envar(rabbitHost).String()
		rabbitPrototocol = fullCommand.Flag("protocol", "The RabbitMQ protocol.").Default("amqp").String()
		rabbitPort       = fullCommand.Flag("port", "The RabbitMQ port.").Default("5672").Int()
		user             = fullCommand.Flag("user", "User used to connect to RabbitMQ. Env="+rabbitUser).Short('u').Default("guest").Envar(rabbitUser).String()
		password         = fullCommand.Flag("password", "Password used to connect to RabbitMQ. Env="+rabbitPassword).Default("guest").Envar(rabbitPassword).String()
		replay           = fullCommand.Flag("replay", "Actually replay the messages to the target Rabbit cluster.").Short('r').Bool()
		maxDepth         = fullCommand.Flag("max-depth", "Maximum depth to find.").Default("5").Int()
		patterns         = fullCommand.Flag("pattern", "Pattern used to find persistent store or index files.").Short('p').Default("*.rdq", "*.idx").Strings()
		threads          = fullCommand.Flag("threads", "Number of parallel threads running.").Short('t').Default(fmt.Sprint((runtime.NumCPU() + 1) / 2)).Int()
		output           = fullCommand.Flag("output", "Specify the output type (Json, Yaml, Hcl)").Short('o').Enum("Hcl", "h", "hcl", "H", "HCL", "Json", "j", "json", "J", "JSON", "Yaml", "Yml", "y", "yml", "yaml", "Y", "YML", "YAML")
		declareQueue     = fullCommand.Flag("declare-queues", "Force queue creation if it does not exist").Bool()
		match            = fullCommand.Flag("match", "Regular expression for matching queues").Short('m').PlaceHolder("regexp").String()
	)

	app.UsageWriter(os.Stdout)
	kingpin.CommandLine = app
	kingpin.CommandLine.HelpFlag.Short('h')
	switch kingpin.Parse() {
	case findLostCommand.FullCommand():

		// Get files in reverse order
		fmt.Println("Finding files")
		files := utils.MustFindFilesMaxDepth(*folder, 1, false, "*.rdq")
		fmt.Printf("Found %v files. Sorting files\n", len(files))
		fileNumbers := []int{}
		for _, file := range files {
			fileNumbers = append(fileNumbers, must(strconv.Atoi(strings.Split(path.Base(file), ".")[0])).(int))
		}
		sort.Sort(sort.Reverse(sort.IntSlice(fileNumbers)))
		for i, fileNumber := range fileNumbers {
			files[i] = path.Join(*folder, fmt.Sprintf("%v.rdq", fileNumber))
		}

		// Create output folder
		os.MkdirAll(*outputFolder, os.ModePerm)

		// Parse configuration file and create output files (faster to create them all here and delete unneeded ones than check if they are created at runtime)
		var lostMessagesData []interface{}
		lostMessagesMap := make(map[string]*LostMessagesData)
		must(collections.ConvertData(string(must(ioutil.ReadFile(*lostMessages)).([]byte)), &lostMessagesData))
		for _, item := range lostMessagesData {
			itemAsMap := item.(json.Dictionary)
			queueName := itemAsMap["name"].(string)
			filePath := path.Join(*outputFolder, queueName)
			lostMessagesMap[queueName] = &LostMessagesData{
				toFind:      itemAsMap["messages"].(int),
				filePath:    filePath,
				fileHandler: must(os.Create(filePath)).(*os.File),
			}
		}

		filesHandled := 0
		// Find messages and write them to the file
		for _, file := range files {
			fmt.Println("Handling file: " + file)
			filesHandled++
			if filesHandled%100 == 0 {
				stillNeedToProcess := false
				fmt.Println("Verifying if we need to keep going")
				for queueName, queueInfo := range lostMessagesMap {
					if queueInfo.found != queueInfo.toFind {
						fmt.Printf("%s is not completed, still missing %v", queueName, queueInfo.toFind-queueInfo.found)
						stillNeedToProcess = true
					}
				}
				if !stillNeedToProcess {
					fmt.Println("Completed!")
					break
				}
			}

			data, err := ReadRabbitFile(file, nil)
			if err != nil {
				panic(err)
			}
			data.ProcessMessages(func(msg *RabbitMessage) {
				queueInfo, ok := lostMessagesMap[msg.Queue]
				if !ok || queueInfo.found == queueInfo.toFind {
					return
				}
				queueInfo.fileHandler.WriteString(fmt.Sprintln(base64.StdEncoding.EncodeToString(msg.Data)))
				queueInfo.found++
			})
		}

		keys := []string{}
		for queueName := range lostMessagesMap {
			keys = append(keys, queueName)
		}
		sort.Strings(keys)

		// Output result and delete unneeded output files (empty)
		for _, queueName := range keys {
			queueInfo := lostMessagesMap[queueName]
			must(queueInfo.fileHandler.Close())
			fmt.Printf("%s, Remaining messages: %v\n", queueName, queueInfo.toFind-queueInfo.found)
			if queueInfo.found == 0 {
				must(os.Remove(queueInfo.filePath))
			}
		}

	case fullCommand.FullCommand():
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
