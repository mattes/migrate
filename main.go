// Package main is the CLI.
// You can use the CLI via Terminal.
// import "github.com/mattes/migrate/migrate" for usage within Go.
package main

import (
	"flag"
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

// Flag to indicate that Usage() should not exit
const NoExit = -1

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
	flags.Usage = func() { Usage(NoExit) }
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
func Usage(status int) {
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

	if status > NoExit {
		os.Exit(status)
	}
}

func verifyMigrationsPath() {
	if MigrationsPath == "" {
		log.Fatal("Migrations path not given.")
	}
}

// Create a new migration in the migration path.
func Create() {
	verifyMigrationsPath()

	name := Args[0]
	if name == "" {
		log.Fatal("Migration name not given.")
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
func Migrate(pipe chan interface{}) {
	verifyMigrationsPath()

	relative, err := strconv.Atoi(Args[0])
	if err != nil {
		log.Fatalf("%q is not a valid number of migrations.", Args[0])
	}

	go migrate.Migrate(pipe, DatabaseURL, MigrationsPath, relative)
}

// Goto migrates the database to a specific version.
func Goto(pipe chan interface{}) {
	verifyMigrationsPath()

	target, err := strconv.Atoi(Args[0])
	if err != nil || target < 0 {
		log.Fatalf("%q is not a valid target version.", Args[0])
	}

	current, err := migrate.Version(DatabaseURL, MigrationsPath)
	if err != nil {
		log.Fatal(err)
	}

	relative := target - int(current)

	go migrate.Migrate(pipe, DatabaseURL, MigrationsPath, relative)
}

// Up runs all up migrations.
func Up(pipe chan interface{}) {
	verifyMigrationsPath()
	go migrate.Up(pipe, DatabaseURL, MigrationsPath)
}

// Down runs all down migrations.
func Down(pipe chan interface{}) {
	verifyMigrationsPath()
	go migrate.Down(pipe, DatabaseURL, MigrationsPath)
}

// Redo rolls back the most recent migration and then applies it again.
func Redo(pipe chan interface{}) {
	verifyMigrationsPath()
	go migrate.Redo(pipe, DatabaseURL, MigrationsPath)
}

// Reset runs all down migrations followed by all up migrations.
func Reset(pipe chan interface{}) {
	verifyMigrationsPath()
	go migrate.Reset(pipe, DatabaseURL, MigrationsPath)
}

// DatabaseVersion shows the current migration version.
func DatabaseVersion() {
	verifyMigrationsPath()

	version, err := migrate.Version(DatabaseURL, MigrationsPath)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(version)
}

func main() {
	Configure()

	if ShowVersion {
		log.Printf("Version %s", Version)
		os.Exit(0)
	}

	start := time.Now()
	pipe := pipep.New()

	switch Command {
	case CommandCreate:
		Create()
		close(pipe)
	case CommandMigrate:
		Migrate(pipe)
	case CommandGoto:
		Goto(pipe)
	case CommandUp:
		Up(pipe)
	case CommandDown:
		Down(pipe)
	case CommandRedo:
		Redo(pipe)
	case CommandReset:
		Reset(pipe)
	case CommandVersion:
		DatabaseVersion()
		close(pipe)
	case CommandHelp:
		Usage(0)
	default:
		Usage(1)
	}

	if ok := writePipe(pipe); ok {
		log.Printf("Total time: %s", time.Since(start))
	} else {
		os.Exit(1)
	}
}

func writePipe(pipe chan interface{}) (ok bool) {
	red := color.New(color.FgRed).SprintFunc()
	green := color.New(color.FgGreen).SprintFunc()
	ok = true

	if pipe != nil {
		for item := range pipe {
			switch item.(type) {
			case error:
				log.Printf("%s\n\n", red(item))
				ok = false

			case file.File:
				f := item.(file.File)

				switch f.Direction {
				case direction.Up:
					log.Printf("%s %s", green(">"), f.FileName)
				case direction.Down:
					log.Printf("%s %s", red("<"), f.FileName)
				}

			default:
				log.Println(item)
			}
		}
	}

	return
}
