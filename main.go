package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	errorCounter uint64

	jobs    int
	logdir  string
	argvars map[string]string
)

var rootCmd = &cobra.Command{
	Use:   "parapara command [args...]",
	Short: "Run commands in parallel",
	Run:   rootCmdHandler,
	Args:  cobra.MinimumNArgs(1),
}

func openFile(name string) (*os.File, error) {
	path := filepath.Join(logdir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Error(err)
	}
	return f, err
}

func worker(ctx context.Context, wg *sync.WaitGroup, id int, name string, args []string) {
	defer wg.Done()

	outFileName := fmt.Sprintf("%s.%d.out", name, id)
	outFile, err := openFile(outFileName)
	if err != nil {
		return
	}
	defer outFile.Close()

	errFileName := fmt.Sprintf("%s.%d.err", name, id)
	errFile, err := openFile(errFileName)
	if err != nil {
		return
	}
	defer errFile.Close()

	for k, v := range argvars {
		k = "{" + k + "}"
		for i, s := range args {
			args[i] = strings.ReplaceAll(s, k, v)
		}
	}

	idString := strconv.Itoa(id)
	for i, s := range args {
		args[i] = strings.ReplaceAll(s, "{#}", idString)
	}

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = outFile
	cmd.Stderr = errFile

	if err := cmd.Run(); err != nil {
		log.Errorf("job %d: %s", id, err)
		atomic.AddUint64(&errorCounter, 1)
	}
}

func rootCmdHandler(cmd *cobra.Command, args []string) {
	_, err := os.Stat(logdir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(logdir, 0777); err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}

	n := args[0]
	var a []string
	if len(args) > 1 {
		a = args[1:]
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go worker(cmd.Context(), wg, i, n, a)
	}
	wg.Wait()
}

func init() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	rootCmd.Flags().IntVarP(&jobs, "jobs", "j", 0, "number of parallel jobs")
	rootCmd.Flags().StringVarP(&logdir, "logdir", "l", cwd, "job output logs directory")
	rootCmd.Flags().StringToStringVarP(&argvars, "var", "v", nil, "additional variables for interpolation in args")
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := rootCmd.ExecuteContext(ctx); err != nil || atomic.LoadUint64(&errorCounter) > 0 {
		os.Exit(1)
	}
}
