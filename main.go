// Package main is the CLI.
// You can use the CLI via Terminal.
// import "github.com/mattes/migrate/migrate" for usage within Go.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/fatih/color"
	_ "github.com/mattes/migrate/driver/bash"
	_ "github.com/mattes/migrate/driver/cassandra"
	_ "github.com/mattes/migrate/driver/crate"
	_ "github.com/mattes/migrate/driver/mysql"
	_ "github.com/mattes/migrate/driver/postgres"
	_ "github.com/mattes/migrate/driver/ql"
	_ "github.com/mattes/migrate/driver/sqlite3"
	"github.com/mattes/migrate/file"
	"github.com/mattes/migrate/migrate"
	"github.com/mattes/migrate/migrate/direction"
	pipep "github.com/mattes/migrate/pipe"
)

// Available commands
const (
	CommandCreate  = "create"
	CommandMigrate = "migrate"
	CommandGoto    = "goto"
	CommandUp      = "up"
	CommandDown    = "down"
	CommandRedo    = "redo"
	CommandReset   = "reset"
	CommandVersion = "version"
	CommandHelp    = "help"
)

// Configuration variables
var (
	// The URL of the database to migrate
	DatabaseURL string

	// The directory containing the migration files
	MigrationsPath string

	// Whether or not to show the migration files
	ShowVersion bool

	// The command given
	Command string

	// The remaining command-line arguments
	Args []string
)

// init overrides the default configuration values with values from the environment.
func init() {
	DatabaseURL = os.Getenv("MIGRATE_URL")
}

// Configure strips the first command-line argument (the command name) and passes the remainder to ConfigureArgs().
func Configure() {
	ConfigureArgs(os.Args[1:])
}

// ConfigureArgs sets the configuration variables from the command-line arguments.  If the arguments could not be parsed, the program exits with an error.
func ConfigureArgs(args []string) {
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flags.Usage = Usage
	flags.StringVar(&DatabaseURL, "url", DatabaseURL, "")
	flags.StringVar(&MigrationsPath, "path", MigrationsPath, "")
	flags.BoolVar(&ShowVersion, "version", ShowVersion, "Show migrate version")
	flags.Parse(args)

	if MigrationsPath == "" {
		MigrationsPath, _ = os.Getwd()
	}

	if flags.NArg() > 0 {
		Command = flags.Arg(0)
		Args = flags.Args()[1:]
	}
}

// Usage prints information about available commands.  This overrides the default output of the -help flag.
func Usage() {
	os.Stderr.WriteString(
		`usage: migrate [-path=<path>] -url=<url> <command> [<args>]

Commands:
   create <name>  Create a new migration
   up             Apply all -up- migrations
   down           Apply all -down- migrations
   reset          Down followed by Up
   redo           Roll back most recent migration, then apply it again
   version        Show current migration version
   migrate <n>    Apply migrations -n|+n
   goto <v>       Migrate to version v
   help           Show this help

'-path' defaults to current working directory.
`)
}

// Create a new migration in the migration path.
func Create() {
	verifyMigrationsPath(MigrationsPath)

	name := Args[0]
	if name == "" {
		log.Fatal("Please specify name.")
	}

	migrationFile, err := migrate.Create(DatabaseURL, MigrationsPath, name)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Version %v migration files created in %v:\n", migrationFile.Version, MigrationsPath)
	log.Println(migrationFile.UpFile.FileName)
	log.Println(migrationFile.DownFile.FileName)
}

// Migrate runs all pending migrations in the migration path.
func Migrate() {
	verifyMigrationsPath(MigrationsPath)

	relativeN := Args[0]

	relativeNInt, err := strconv.Atoi(relativeN)
	if err != nil {
		log.Fatal("Unable to parse param <n>.")
	}

	timerStart = time.Now()
	pipe := pipep.New()
	go migrate.Migrate(pipe, DatabaseURL, MigrationsPath, relativeNInt)
	ok := writePipe(pipe)
	printTimer()

	if !ok {
		os.Exit(1)
	}
}

// Goto migrates the database to a specific version.
func Goto() {
	verifyMigrationsPath(MigrationsPath)

	toVersion := Args[0]
	toVersionInt, err := strconv.Atoi(toVersion)
	if err != nil || toVersionInt < 0 {
		log.Fatal("Unable to parse param <v>.")
	}

	currentVersion, err := migrate.Version(DatabaseURL, MigrationsPath)
	if err != nil {
		log.Fatal(err)
	}

	relativeNInt := toVersionInt - int(currentVersion)

	timerStart = time.Now()
	pipe := pipep.New()
	go migrate.Migrate(pipe, DatabaseURL, MigrationsPath, relativeNInt)
	ok := writePipe(pipe)
	printTimer()

	if !ok {
		os.Exit(1)
	}
}

// Up runs all up migrations.
func Up() {
	verifyMigrationsPath(MigrationsPath)

	timerStart = time.Now()
	pipe := pipep.New()
	go migrate.Up(pipe, DatabaseURL, MigrationsPath)
	ok := writePipe(pipe)
	printTimer()

	if !ok {
		os.Exit(1)
	}
}

// Down runs all down migrations.
func Down() {
	verifyMigrationsPath(MigrationsPath)

	timerStart = time.Now()
	pipe := pipep.New()
	go migrate.Down(pipe, DatabaseURL, MigrationsPath)
	ok := writePipe(pipe)
	printTimer()

	if !ok {
		os.Exit(1)
	}
}

// Redo rolls back the most recent migration and then applies it again.
func Redo() {
	verifyMigrationsPath(MigrationsPath)

	timerStart = time.Now()
	pipe := pipep.New()
	go migrate.Redo(pipe, DatabaseURL, MigrationsPath)
	ok := writePipe(pipe)
	printTimer()

	if !ok {
		os.Exit(1)
	}
}

// Reset runs all down migrations followed by all up migrations.
func Reset() {
	verifyMigrationsPath(MigrationsPath)

	timerStart = time.Now()
	pipe := pipep.New()
	go migrate.Reset(pipe, DatabaseURL, MigrationsPath)
	ok := writePipe(pipe)
	printTimer()

	if !ok {
		os.Exit(1)
	}
}

// Version shows the current migration version.
func DatabaseVersion() {
	verifyMigrationsPath(MigrationsPath)

	version, err := migrate.Version(DatabaseURL, MigrationsPath)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(version)
}

func main() {
	Configure()

	if ShowVersion {
		fmt.Println(Version)
		os.Exit(0)
	}

	switch Command {
	case CommandCreate:
		Create()

	case CommandMigrate:
		Migrate()

	case CommandGoto:
		Goto()

	case CommandUp:
		Up()

	case CommandDown:
		Down()

	case CommandRedo:
		Redo()

	case CommandReset:
		Reset()

	case CommandVersion:
		DatabaseVersion()

	case CommandHelp:
		Usage()

	default:
		Usage()
		os.Exit(1)
	}
}

func writePipe(pipe chan interface{}) (ok bool) {
	okFlag := true
	if pipe != nil {
		for {
			select {
			case item, more := <-pipe:
				if !more {
					return okFlag
				} else {
					switch item.(type) {

					case string:
						fmt.Println(item.(string))

					case error:
						c := color.New(color.FgRed)
						c.Println(item.(error).Error(), "\n")
						okFlag = false

					case file.File:
						f := item.(file.File)
						if f.Direction == direction.Up {
							c := color.New(color.FgGreen)
							c.Print(">")
						} else if f.Direction == direction.Down {
							c := color.New(color.FgRed)
							c.Print("<")
						}
						fmt.Printf(" %s\n", f.FileName)

					default:
						text := fmt.Sprint(item)
						fmt.Println(text)
					}
				}
			}
		}
	}
	return okFlag
}

func verifyMigrationsPath(path string) {
	if path == "" {
		fmt.Println("Please specify path")
		os.Exit(1)
	}
}

var timerStart time.Time

func printTimer() {
	diff := time.Now().Sub(timerStart).Seconds()
	if diff > 60 {
		fmt.Printf("\n%.4f minutes\n", diff/60)
	} else {
		fmt.Printf("\n%.4f seconds\n", diff)
	}
}
