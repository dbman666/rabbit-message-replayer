package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/coveo/gotemplate/v3/collections"
	"github.com/coveo/gotemplate/v3/errors"
	"github.com/coveo/gotemplate/v3/hcl"
	"github.com/coveo/gotemplate/v3/json"
	"github.com/coveo/gotemplate/v3/utils"
	"github.com/coveo/gotemplate/v3/yaml"
	"github.com/coveo/kingpin/v2"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/streadway/amqp"
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
	print      = utils.ColorPrint
	printf     = utils.ColorPrintf
	println    = utils.ColorPrintln
	errPrintf  = utils.ColorErrorPrintf
	errPrintln = utils.ColorErrorPrintln
	must       = errors.Must
	iif        = collections.IIf
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
		app              = kingpin.New(os.Args[0], description).AutoShortcut()
		getVersion       = app.Flag("version", "Get the current version of the replayer").Short('v').Bool()
		colorModeIsSet   bool
		colorMode        = app.Flag("color", "Force rendering of colors event if output is redirected.").IsSetByUser(&colorModeIsSet).Bool()
		folder           = app.Flag("folder", "Folder where to find messages.").Short('f').ExistingDir()
		rabbitURL        = app.Flag("rabbit-host", "The RabbitMQ Url. Env="+rabbitHost).Short('H').Envar(rabbitHost).String()
		rabbitPrototocol = app.Flag("protocol", "The RabbitMQ protocol.").Default("amqp").String()
		rabbitPort       = app.Flag("port", "The RabbitMQ port.").Default("5672").NoAutoShortcut().Int()
		user             = app.Flag("user", "User used to connect to RabbitMQ. Env="+rabbitUser).Short('u').Default("guest").Envar(rabbitUser).String()
		password         = app.Flag("password", "Password used to connect to RabbitMQ. Env="+rabbitPassword).Default("guest").NoAutoShortcut().Envar(rabbitPassword).String()
		declareQueue     = app.Flag("declare-queues", "Force queue creation if it does not exist").Bool()
		isExchange       = app.Flag("is-exchange", "Set to true if the queue names are actually exchanges").Bool()
		match            = app.Flag("match", "Regular expression for matching queues").Short('m').PlaceHolder("regexp").String()
		maxDepth         = app.Flag("max-depth", "Maximum depth to find.").Default("5").Int()
		outputFolder     = app.Flag("output-folder", "Where queue data should be exported").String()
		threads          = app.Flag("threads", "Number of parallel threads running.").Short('t').Default(fmt.Sprint((runtime.NumCPU() + 1) / 2)).Int()
		verbose          = app.Flag("verbose", "Indicate to add traces during processing").Short('V').Bool()
		patterns         = app.Flag("pattern", "Pattern used to find persistent store or index files.").Short('p').Default("*.rdq", "*.idx").Strings()

		findLostCommand = app.Command("find-lost", "Finds lost messages given a list of queues and how many messages they have lost")
		lostMessages    = findLostCommand.Flag("lost-messages", "Map of lost messages by queue").Required().ExistingFile()
		start           = findLostCommand.Flag("starts-with", "File number to start with").Int()

		splitCommand = app.Command("split-messages", "Finds lost messages given a list of queues and how many messages they have lost")

		replayCommand = app.Command("replay", "Replay messages that have been extracted by find-lost command")

		fullCommand = app.Command("full", "Parse all files recursively in the source folder to find messages")
		replay      = fullCommand.Flag("replay", "Actually replay the messages to the target Rabbit cluster.").Short('r').Bool()
		output      = fullCommand.Flag("output", "Specify the output type (Json, Yaml, Hcl)").Short('o').Enum("Hcl", "h", "hcl", "H", "HCL", "Json", "j", "json", "J", "JSON", "Yaml", "Yml", "y", "yml", "yaml", "Y", "YML", "YAML")
	)

	app.UsageWriter(os.Stdout)
	kingpin.CommandLine = app
	kingpin.CommandLine.HelpFlag.Short('h')
	command := kingpin.Parse()

	if *getVersion {
		println(version)
		os.Exit(0)
	}

	if colorModeIsSet {
		color.NoColor = *colorMode
	}

	if *threads == 0 {
		*threads = runtime.NumCPU() / 2
	}

	var re *regexp.Regexp
	if *match != "" {
		re = regexp.MustCompile(*match)
	}

	var patternList []string
	for _, p := range *patterns {
		patternList = append(patternList, strings.Split(p, ";")...)
	}

	var files []string
	if command == findLostCommand.FullCommand() || command == splitCommand.FullCommand() {
		if *outputFolder == "" {
			panic("You need to specify an output folder")
		}
		// Get files in reverse order
		errPrintln(color.GreenString("Finding files"))
		files = utils.MustFindFilesMaxDepth(*folder, *maxDepth, false, patternList...)
		errPrintf(color.GreenString("Found %v files. Sorting files\n"), len(files))

		// Create output folder
		os.MkdirAll(*outputFolder, os.ModePerm)
	}

	switch command {
	case findLostCommand.FullCommand():
		fileNumbers := []int{}
		for _, file := range files {
			fileNumbers = append(fileNumbers, must(strconv.Atoi(strings.Split(path.Base(file), ".")[0])).(int))
		}
		sort.Sort(sort.Reverse(sort.IntSlice(fileNumbers)))
		for i, fileNumber := range fileNumbers {
			if *start > 0 && fileNumber > *start {
				files[i] = ""
				continue
			}
			files[i] = path.Join(*folder, fmt.Sprintf("%v.rdq", fileNumber))
		}

		// Parse configuration file and create output files (faster to create them all here and delete unneeded ones than check if they are created at runtime)
		var lostMessagesData []interface{}

		type FindData struct {
			toFind      int
			found       int
			pushAPI     int
			done        bool
			filePath    string
			fileHandler *os.File
		}

		lostMessagesMap := make(map[string]*FindData)
		must(collections.ConvertData(string(must(ioutil.ReadFile(*lostMessages)).([]byte)), &lostMessagesData))
		for _, item := range lostMessagesData {
			itemAsMap := item.(json.Dictionary)
			queueName := itemAsMap["name"].(string)
			filePath := path.Join(*outputFolder, queueName)
			lostMessagesMap[queueName] = &FindData{
				toFind:      itemAsMap["messages"].(int),
				filePath:    filePath,
				fileHandler: must(os.Create(filePath)).(*os.File),
			}
		}

		filesHandled := 0
		// Find messages and write them to the file
		for _, file := range files {
			if file == "" {
				continue
			}
			stillNeedToProcess := false
			for queueName, queueInfo := range lostMessagesMap {
				if queueInfo.found < queueInfo.toFind {
					stillNeedToProcess = true
				} else if !queueInfo.done {
					queueInfo.done = true
					fmt.Printf(color.GreenString("All messages in %s have been found\n"), queueName)
				}
			}
			if !stillNeedToProcess {
				break
			}
			errPrintln(color.GreenString("Handling file: " + file))
			filesHandled++

			data, err := ReadRabbitFile(file, nil)
			if err != nil {
				errPrintln(color.RedString(err.Error()))
				continue
			}
			data.ProcessMessages(func(msg *RabbitMessage) {
				if queueInfo, ok := lostMessagesMap[msg.Queue]; ok && !queueInfo.done {
					if msg.IsPush() {
						queueInfo.pushAPI++
					}
					queueInfo.fileHandler.WriteString(fmt.Sprintln(base64.StdEncoding.EncodeToString(msg.Data)))
					queueInfo.found++
				}
			})
		}
		errPrintln(color.GreenString("Completed!"))

		keys := []string{}
		for queueName := range lostMessagesMap {
			keys = append(keys, queueName)
		}
		sort.Strings(keys)

		table := getTable("Queue name", "To find", "Found", "PushAPI", "Crawlers", "Missing/Over")

		// Output result and delete unneeded output files (empty)
		var toFind, found, pushAPI int
		for _, queueName := range keys {
			queueInfo := lostMessagesMap[queueName]
			must(queueInfo.fileHandler.Close())
			data := collections.NewList(queueName, queueInfo.toFind, queueInfo.found, queueInfo.pushAPI, queueInfo.found-queueInfo.pushAPI, queueInfo.found-queueInfo.toFind)

			toFind += queueInfo.toFind
			found += queueInfo.found
			pushAPI += queueInfo.pushAPI
			table.Append(data.Strings())
			if queueInfo.found == 0 {
				must(os.Remove(queueInfo.filePath))
			}
		}
		data := collections.NewList("", toFind, found, pushAPI, found-pushAPI, found-toFind)
		table.SetFooter(data.Strings())
		table.Render()
		fmt.Println()

	case splitCommand.FullCommand():
		type WriteData struct {
			file  string
			value string
		}

		numThreads := int(math.Min(float64(len(files)), float64(*threads)))
		errPrintln(color.GreenString("Reading with %v threads!\n", numThreads))

		filesToHandle := make(chan string, numThreads)
		toWrite := make(chan WriteData)
		doneReading := make(chan bool, numThreads)
		doneWriting := make(chan bool, 1)
		var count int32

		for i := 0; i < numThreads; i++ {
			go func() {
				for {
					file, more := <-filesToHandle
					if more {
						atomic.AddInt32(&count, 1)
						errPrintln(color.GreenString(" - Reading file " + file))
						data := must(ReadRabbitFile(file, nil)).(RabbitFile)
						data.ProcessMessages(func(msg *RabbitMessage) {
							if re == nil || re.MatchString(msg.Queue) {
								toWrite <- WriteData{file: msg.Queue, value: fmt.Sprintln(base64.StdEncoding.EncodeToString(msg.Data))}
							}
						})
					} else {
						doneReading <- true
						return
					}
				}
			}()
		}

		fileHandlers := make(map[string]*os.File)
		go func() {
			for {
				writeData, more := <-toWrite
				if more {
					path := path.Join(*outputFolder, writeData.file)
					fileHandle := fileHandlers[path]
					if fileHandle == nil {
						fileHandle = must(os.Create(path)).(*os.File)
						fileHandlers[path] = fileHandle
					}
					fileHandle.WriteString(writeData.value)
				} else {
					doneWriting <- true
					return
				}
			}
		}()

		for _, file := range files {
			filesToHandle <- file
		}
		close(filesToHandle)

		for i := 0; i < numThreads; i++ {
			<-doneReading
		}
		errPrintln(color.GreenString("Read %v files!\n", atomic.LoadInt32(&count)))
		close(toWrite)

		<-doneWriting
		errPrintln(color.GreenString("Done writing!"))

	case replayCommand.FullCommand():
		url := fmt.Sprintf("%s://%s:%s@%s:%d", *rabbitPrototocol, *user, *password, *rabbitURL, *rabbitPort)
		publish := make(chan *RabbitMessage)
		completed := make(chan publisherStatus)
		go messageHandler(0, url, publish, completed, *declareQueue, *isExchange)
		files := utils.MustFindFilesMaxDepth(*folder, 1, false, "*")
		for _, fileName := range files {
			fmt.Println("Processing file", fileName)
			file := must(os.Open(fileName)).(*os.File)
			defer file.Close()

			reader := bufio.NewReader(file)
			for {
				line, err := reader.ReadString('\n')
				if err == io.EOF {
					break
				}
				publish <- &RabbitMessage{
					Queue: filepath.Base(fileName),
					Data:  must(base64.StdEncoding.DecodeString(line)).([]byte),
				}
			}
		}
		close(publish)
		fmt.Println("Waiting for publisher to complete")
		status := <-completed
		table := getTable("Queue name", "Published")
		var total int
		for queue, published := range status.published {
			table.Append(collections.NewList(queue, published).Strings())
			total += published
		}
		table.SetFooter(collections.NewList("", total).Strings())
		table.Render()
		fmt.Println()

	case fullCommand.FullCommand():

		files := utils.MustFindFilesMaxDepth(*folder, *maxDepth, false, patternList...)
		files = collections.AsList(files).Unique().Strings()
		if *verbose {
			errPrintf(color.GreenString("%d %s on %d thread(s)\n", len(files), "file(s) to process", *threads))
		}

		url := fmt.Sprintf("%s://%s:%s@%s:%d", *rabbitPrototocol, *user, *password, *rabbitURL, *rabbitPort)

		// Start multithreads processing
		jobs := make(chan string, *threads)
		results := make(chan RabbitFile, len(files))
		completed := make(chan publisherStatus)
		var publish chan *RabbitMessage
		if *replay {
			publish = make(chan *RabbitMessage, *threads*30)
		}
		for i := 0; i < *threads; i++ {
			go fileHandler(i, jobs, results, re)

			if *replay {
				go messageHandler(i, url, publish, completed, *declareQueue, *isExchange)
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

				table := getTable(columns.Strings()...)

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

		if publish != nil {
			close(publish)
		}

		if *replay {
			for i := 0; i < *threads; i++ {
				<-completed
			}
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

type publisherStatus struct {
	id        int
	published map[string]int
}

func messageHandler(id int, url string, messages <-chan *RabbitMessage, completed chan publisherStatus, declareQueues bool, isExchange bool) {
	conn := must(amqp.Dial(url)).(*amqp.Connection)
	defer conn.Close()

	ch := must(conn.Channel()).(*amqp.Channel)
	defer ch.Close()

	go func() {
		returned := ch.NotifyReturn(make(chan amqp.Return, 1))
		for r := range returned {
			errPrintln(color.RedString("Returned message"), r.RoutingKey)
			errPrintln(color.RedString("Error"), r.ReplyText)
		}
	}()

	published := make(map[string]int)

	defer func() {
		if completed != nil {
			completed <- publisherStatus{id, published}
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
				"cmf": fmt.Sprintf("{url:%s,method:%s,zip:true}", msg.Queue, msg.Method),
			}
		}
		if isExchange {
			must(ch.Publish(msg.Queue, "", true, false, pub))
		} else {
			must(ch.Publish("", msg.Queue, true, false, pub))
		}
		published[msg.Queue]++
	}
}

func getTable(columns ...string) *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetRowSeparator("-")
	table.SetColumnSeparator(" ")
	table.SetCenterSeparator(" ")
	table.SetHeader(columns)
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
	return table
}
