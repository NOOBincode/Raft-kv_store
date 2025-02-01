package util

import (
	"log"
)

const (
	//LevelError only
	LevelError = 1
	//LevelWarning and error
	LevelWarning = 2
	// LevelInfo LevelInfo,warning and error
	LevelInfo = 3
	// LevelTrace Info,warning,error and trace
	LevelTrace = 4
	// LevelVerbose All
	LevelVerbose = 5
)

var logger = log.New(log.Writer(), log.Prefix(), log.Flags())
var Loglevel = LevelInfo

func setLogLevel(level int) {
	if level < LevelError {
		level = LevelError
	}
	Loglevel = level
}
func WriteLog(level int, format string, v ...interface{}) {
	if level <= Loglevel {
		logger.Printf(format, v...)
	}
}

// WriteError writes an error log
func WriteError(format string, v ...interface{}) {
	WriteLog(LevelError, format, v...)
}

// WriteWarning writes a warning log
func WriteWarning(format string, v ...interface{}) {
	WriteLog(LevelWarning, format, v...)
}

// WriteInfo writes a information
func WriteInfo(format string, v ...interface{}) {
	WriteLog(LevelInfo, format, v...)
}

// WriteTrace writes traces and debug information
func WriteTrace(format string, v ...interface{}) {
	WriteLog(LevelTrace, format, v...)
}

// WriteVerbose writes verbose infromation
func WriteVerbose(format string, v ...interface{}) {
	WriteLog(LevelVerbose, format, v...)
}

// Panicf is equivalent to l.Printf() followed by a call to panic().
func Panicf(format string, v ...interface{}) {
	logger.Panicf(format, v...)
}

// Panicln is equivalent to l.Println() followed by a call to panic().
func Panicln(v ...interface{}) {
	logger.Panicln(v...)
}

// Fatalf is equivalent to Printf followed by os.Exit(1)
func Fatalf(format string, v ...interface{}) {
	logger.Fatalf(format, v...)
}
